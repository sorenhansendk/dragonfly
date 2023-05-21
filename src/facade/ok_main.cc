// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/strings/ascii.h>

#include "base/hash.h"
#include "base/init.h"
#include "facade/command_id.h"
#include "facade/command_registry.h"
#include "facade/conn_context.h"
#include "facade/dragonfly_listener.h"
#include "facade/error.h"
#include "facade/redis_parser.h"
#include "facade/reply_builder.h"
#include "facade/service_interface.h"
#include "util/accept_server.h"
#include "util/fibers/dns_resolve.h"
#include "util/fibers/pool.h"

ABSL_FLAG(uint32_t, port, 6379, "server port");

using namespace util;
using namespace std;
using absl::GetFlag;
using namespace boost::asio;

namespace facade {

namespace {

thread_local ConnectionStats tl_stats;

inline void ToUpper(const MutableSlice* val) {
  for (auto& c : *val) {
    c = absl::ascii_toupper(c);
  }
}

inline uint16_t Shard(std::string_view v, uint16_t shard_num) {
  XXH64_hash_t hash = XXH64(v.data(), v.size(), 1108202024061983ULL);
  return hash % shard_num;
}

class ProxyContext : public ConnectionContext {
 public:
  ProxyContext(util::FiberSocketBase* peer, Connection* owner) : ConnectionContext(peer, owner) {
  }

  string upstream_id;
};

class MyCommandId : public CommandId {
 public:
  using CommandId::CommandId;
  using Handler =
      fu2::function_base<true /*owns*/, true /*copyable*/, fu2::capacity_default,
                         false /* non-throwing*/, false /* strong exceptions guarantees*/,
                         void(CmdArgList, ProxyContext*) const>;

  MyCommandId& SetHandler(Handler f) {
    handler_ = std::move(f);
    return *this;
  }

  void Invoke(CmdArgList args, ProxyContext* cntx) const {
    handler_(std::move(args), cntx);
  }

  bool HasHandler() const {
    return bool(handler_);
  }

 private:
  Handler handler_;
};

struct HostPort {
  string host;
  uint16_t port;
};

class UpstreamConnection {
 public:
  UpstreamConnection(string_view host, uint16_t port)
      : host_port_{string(host), port}, socket_(nullptr), parser_(false) {
  }

  error_code Connect(fb2::ProactorBase* pb);

  io::Result<string> WriteReq(string_view req);

 private:
  error_code ReadRespReply(base::IoBuf* io_buf, uint32_t* consumed);

  HostPort host_port_;
  std::unique_ptr<FiberSocketBase> socket_;
  RedisParser parser_;
};

class ServiceShard {
 public:
  static void InitThreadLocal(fb2::ProactorBase* pb);

  error_code AddUpstream(string_view id, string_view host, uint16_t port);

  static ServiceShard* tlocal() {
    return shard_;
  }

  UpstreamConnection* GetUpstream() {
    CHECK(!conns_.empty());
    return conns_.begin()->second;
  }

  bool HasUpstream() const {
    return !conns_.empty();
  }

 private:
  ServiceShard(fb2::ProactorBase* pb) : proactor_(pb) {
  }

  absl::flat_hash_map<string, UpstreamConnection*> conns_;
  fb2::ProactorBase* proactor_;

  static __thread ServiceShard* shard_;
};

class OkService : public ServiceInterface {
 public:
  OkService(ProactorPool* pp) : pool_(pp) {
  }

  void DispatchCommand(CmdArgList args, ConnectionContext* cntx) final;

  void DispatchMC(const MemcacheParser::Command& cmd, std::string_view value,
                  ConnectionContext* cntx) final {
    cntx->reply_builder()->SendError("");
  }

  ConnectionContext* CreateContext(util::FiberSocketBase* peer, Connection* owner) final {
    return new ProxyContext{peer, owner};
  }

  ConnectionStats* GetThreadLocalConnectionStats() final {
    return &tl_stats;
  }

  void Init();

 private:
  void Upstream(CmdArgList args, ConnectionContext* cntx);

  CommandRegistry<MyCommandId> registry_;

  util::ProactorPool* pool_;
};

__thread ServiceShard* ServiceShard::shard_ = nullptr;

error_code UpstreamConnection::Connect(fb2::ProactorBase* pb) {
  socket_.reset(pb->CreateSocket());
  auto address = ip::make_address(host_port_.host);

  FiberSocketBase::endpoint_type ep(address, host_port_.port);
  return socket_->Connect(ep);
}

io::Result<string> UpstreamConnection::WriteReq(string_view req) {
  VLOG(1) << "Sending upstream request " << req;

  ReqSerializer serializer{socket_.get()};

  serializer.SendCommand(req);
  error_code ec = serializer.ec();
  if (ec) {
    return nonstd::make_unexpected(ec);
  }
  base::IoBuf io_buf{128};
  uint32_t consumed = 0;
  ec = ReadRespReply(&io_buf, &consumed);
  if (ec)
    return nonstd::make_unexpected(ec);

  return "OK";
}

error_code UpstreamConnection::ReadRespReply(base::IoBuf* io_buf, uint32_t* consumed) {
  error_code ec;

  // TODO: to pass it back
  RespVec resp_args;
  // basically reflection of dragonfly_connection IoLoop function.
  while (!ec) {
    io::MutableBytes buf = io_buf->AppendBuffer();
    io::Result<size_t> size_res = socket_->Recv(buf);
    if (!size_res)
      return size_res.error();

    VLOG(2) << "Read master response of " << *size_res << " bytes";

    io_buf->CommitWrite(*size_res);

    RedisParser::Result result = parser_.Parse(io_buf->InputBuffer(), consumed, &resp_args);

    VLOG(1) << "Read: " << ToSV(io_buf->InputBuffer()) << " result " << result << " consumed "
            << *consumed;

    if (result == RedisParser::OK && !resp_args.empty()) {
      return error_code{};  // success path
    }

    if (result != RedisParser::INPUT_PENDING) {
      LOG(ERROR) << "Invalid parser status " << result << " for buffer of size "
                 << io_buf->InputLen();
      return std::make_error_code(std::errc::bad_message);
    }
    io_buf->ConsumeInput(*consumed);
  }

  return ec;
}

void ServiceShard::InitThreadLocal(fb2::ProactorBase* pb) {
  shard_ = new ServiceShard(pb);
}

error_code ServiceShard::AddUpstream(string_view id, string_view host, uint16_t port) {
  auto [it, inserted] = conns_.insert({string(id), nullptr});
  if (!inserted) {
    return make_error_code(errc::connection_already_in_progress);
  }

  it->second = new UpstreamConnection{host, port};
  error_code ec = it->second->Connect(proactor_);
  if (ec) {
    delete it->second;
    conns_.erase(it);
    return ec;
  }
  VLOG(1) << "Added upstream " << id << " " << host << ":" << port;
  return error_code{};
}

// TODO: to make it work with redis protocol.
string MakeCmdString(CmdArgList args) {
  string res;
  for (auto& arg : args) {
    res.append(arg.data(), arg.size());
    res.push_back(' ');
  }
  return res;
}

void OkService::DispatchCommand(CmdArgList args, ConnectionContext* cntx) {
  ToUpper(&args[0]);
  string_view cmd_str = ArgS(args, 0);

  const MyCommandId* cid = registry_.Find(cmd_str);
  ProxyContext* pc = reinterpret_cast<ProxyContext*>(cntx);

  if (!cid) {
    return (*cntx)->SendError(kSyntaxErr);
  }

  if ((cid->arity() > 0 && args.size() != size_t(cid->arity())) ||
      (cid->arity() < 0 && args.size() < size_t(-cid->arity()))) {
    (*cntx)->SendError(WrongNumArgsError(cmd_str), kSyntaxErrType);
    return;
  }

  if (cid->HasHandler()) {
    args.remove_prefix(1);
    cid->Invoke(std::move(args), reinterpret_cast<ProxyContext*>(cntx));
  } else if (ServiceShard::tlocal()->HasUpstream()) {
    if (cid->first_key_pos() > 0) {
      // Absolutely dumb heuristic to choose an upstream connection.
      string_view key = ArgS(args, cid->first_key_pos());
      uint16_t shard = Shard(key, pool_->size());

      VLOG(1) << "Dispatching by key " << key << " to shard " << shard;

      auto res = pool_->at(shard)->Await([args] {
        io::Result<string> res =
            ServiceShard::tlocal()->GetUpstream()->WriteReq(MakeCmdString(args));
        return res;
      });

      if (res) {
        (*cntx)->SendOk();
      } else {
        LOG(ERROR) << "Failed to write to upstream " << res.error();
        (*cntx)->SendError(res.error().message());
      }
    }
  } else {
    return (*cntx)->SendOk();
  }
}

void OkService::Upstream(CmdArgList args, ConnectionContext* cntx) {
  ToUpper(&args[0]);
  string_view sub_cmd = ArgS(args, 0);
  if (sub_cmd == "ADD") {
    // UPSTREAM ADD id host port
    if (args.size() != 4) {
      return (*cntx)->SendError(WrongNumArgsError("upstream"));
    }
    string_view id = ArgS(args, 1);
    string_view host = ArgS(args, 2);
    uint32_t port = 0;
    if (!absl::SimpleAtoi(ArgS(args, 3), &port) || port == 0 || port > kuint16max) {
      return (*cntx)->SendError(kUintErr);
    }

    fb2::Mutex mu;
    error_code global_ec;
    pool_->AwaitFiberOnAll([&](auto*) {
      auto ec = ServiceShard::tlocal()->AddUpstream(id, host, port);
      if (ec) {
        unique_lock lk(mu);
        global_ec = ec;
      }
    });

    if (global_ec) {
      return (*cntx)->SendError(global_ec.message());
    }

    return (*cntx)->SendOk();
  }

  (*cntx)->SendOk();
}

#define MFUNC(x) \
  SetHandler([this](CmdArgList sp, ConnectionContext* cntx) { this->x(std::move(sp), cntx); })

void OkService::Init() {
  pool_->DispatchBrief([this](fb2::ProactorBase* pb) { ServiceShard::InitThreadLocal(pb); });

  using CI = MyCommandId;

  registry_ << CI{"UPSTREAM", 0, -2, 0, 0, 0}.MFUNC(Upstream) << CI{"SET", 0, -3, 1, 1, 1};
}

void RunEngine(ProactorPool* pool, AcceptServer* acceptor) {
  OkService service(pool);

  service.Init();

  acceptor->AddListener(GetFlag(FLAGS_port), new Listener{Protocol::REDIS, &service});

  acceptor->Run();
  acceptor->Wait();
}

}  // namespace

}  // namespace facade

int main(int argc, char* argv[]) {
  MainInitGuard guard(&argc, &argv);

  CHECK_GT(GetFlag(FLAGS_port), 0u);

  unique_ptr<util::ProactorPool> pp(fb2::Pool::IOUring(1024));
  pp->Run();

  AcceptServer acceptor(pp.get());
  facade::RunEngine(pp.get(), &acceptor);

  pp->Stop();

  return 0;
}
