// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/list_family.h"

extern "C" {
#include "redis/object.h"
#include "redis/sds.h"
}

#include <absl/strings/numbers.h>

#include "base/logging.h"
#include "server/blocking_controller.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/server_state.h"
#include "server/transaction.h"

/**
 * The number of entries allowed per internal list node can be specified
 * as a fixed maximum size or a maximum number of elements.
 * For a fixed maximum size, use -5 through -1, meaning:
 * -5: max size: 64 Kb  <-- not recommended for normal workloads
 * -4: max size: 32 Kb  <-- not recommended
 * -3: max size: 16 Kb  <-- probably not recommended
 * -2: max size: 8 Kb   <-- good
 * -1: max size: 4 Kb   <-- good
 * Positive numbers mean store up to _exactly_ that number of elements
 * per list node.
 * The highest performing option is usually -2 (8 Kb size) or -1 (4 Kb size),
 * but if your use case is unique, adjust the settings as necessary.
 *
 */
DEFINE_int32(list_max_listpack_size, -2, "Maximum listpack size, default is 8kb");

/**
 * Lists may also be compressed.
 * Compress depth is the number of quicklist listpack nodes from *each* side of
 * the list to *exclude* from compression.  The head and tail of the list
 * are always uncompressed for fast push/pop operations.  Settings are:
 * 0: disable all list compression
 * 1: depth 1 means "don't start compressing until after 1 node into the list,
 *    going from either the head or tail"
 *    So: [head]->node->node->...->node->[tail]
 *    [head], [tail] will always be uncompressed; inner nodes will compress.
 * 2: [head]->[next]->node->node->...->node->[prev]->[tail]
 *    2 here means: don't compress head or head->next or tail->prev or tail,
 *    but compress all nodes between them.
 * 3: [head]->[next]->[next]->node->node->...->node->[prev]->[prev]->[tail]
 * etc.
 *
 */

DEFINE_int32(list_compress_depth, 0, "Compress depth of the list. Default is no compression");

namespace dfly {

using namespace std;
using namespace facade;

namespace {

quicklistEntry QLEntry() {
  quicklistEntry res{.quicklist = NULL,
                     .node = NULL,
                     .zi = NULL,
                     .value = NULL,
                     .longval = 0,
                     .sz = 0,
                     .offset = 0};
  return res;
}

quicklist* GetQL(const PrimeValue& mv) {
  return (quicklist*)mv.RObjPtr();
}

void* listPopSaver(unsigned char* data, size_t sz) {
  return createStringObject((char*)data, sz);
}

string ListPop(ListDir dir, quicklist* ql) {
  long long vlong;
  robj* value = NULL;

  int ql_where = (dir == ListDir::LEFT) ? QUICKLIST_HEAD : QUICKLIST_TAIL;

  // Empty list automatically removes the key (see below).
  CHECK_EQ(1,
           quicklistPopCustom(ql, ql_where, (unsigned char**)&value, NULL, &vlong, listPopSaver));
  string res;
  if (value) {
    DCHECK(value->encoding == OBJ_ENCODING_EMBSTR || value->encoding == OBJ_ENCODING_RAW);
    sds s = (sds)(value->ptr);
    res = string{s, sdslen(s)};
    decrRefCount(value);
  } else {
    res = absl::StrCat(vlong);
  }

  return res;
}

bool ElemCompare(const quicklistEntry& entry, string_view elem) {
  if (entry.value) {
    return entry.sz == elem.size() &&
           (entry.sz == 0 || memcmp(entry.value, elem.data(), entry.sz) == 0);
  }

  absl::AlphaNum an(entry.longval);
  return elem == an.Piece();
}

using FFResult = pair<PrimeKey, unsigned>;  // key, argument index.

struct ShardFFResult {
  PrimeKey key;
  ShardId sid = kInvalidSid;
};

OpResult<ShardFFResult> FindFirst(Transaction* trans) {
  VLOG(2) << "FindFirst::Find " << trans->DebugId();

  // Holds Find results: (iterator to a found key, and its index in the passed arguments).
  // See DbSlice::FindFirst for more details.
  // spans all the shards for now.
  std::vector<OpResult<FFResult>> find_res(shard_set->size());
  fill(find_res.begin(), find_res.end(), OpStatus::KEY_NOTFOUND);

  auto cb = [&find_res](auto* t, EngineShard* shard) {
    auto args = t->ShardArgsInShard(shard->shard_id());
    OpResult<pair<PrimeIterator, unsigned>> ff_res =
        shard->db_slice().FindFirst(t->db_index(), args);

    if (ff_res) {
      FFResult ff_result(ff_res->first->first.AsRef(), ff_res->second);
      find_res[shard->shard_id()] = move(ff_result);
    } else {
      find_res[shard->shard_id()] = ff_res.status();
    }
    return OpStatus::OK;
  };

  trans->Execute(move(cb), false);

  uint32_t min_arg_indx = UINT32_MAX;

  ShardFFResult shard_result;

  for (size_t sid = 0; sid < find_res.size(); ++sid) {
    const auto& fr = find_res[sid];
    auto status = fr.status();
    if (status == OpStatus::KEY_NOTFOUND)
      continue;

    if (status == OpStatus::WRONG_TYPE) {
      return status;
    }

    CHECK(fr);

    const auto& it_pos = fr.value();

    size_t arg_indx = trans->ReverseArgIndex(sid, it_pos.second);
    if (arg_indx < min_arg_indx) {
      min_arg_indx = arg_indx;
      shard_result.sid = sid;

      // we do not dereference the key, do not extract the string value, so it it
      // ok to just move it. We can not dereference it due to limitations of SmallString
      // that rely on thread-local data-structure for pointer translation.
      shard_result.key = it_pos.first.AsRef();
    }
  }

  if (shard_result.sid == kInvalidSid) {
    return OpStatus::KEY_NOTFOUND;
  }

  return OpResult<ShardFFResult>{move(shard_result)};
}

class BPopper {
 public:
  explicit BPopper(ListDir dir);

  // Returns WRONG_TYPE, OK.
  // If OK is returned then use result() to fetch the value.
  OpStatus Run(Transaction* t, unsigned msec);

  // returns (key, value) pair.
  auto result() const {
    return make_pair<string_view, string_view>(key_, value_);
  }

 private:
  OpStatus Pop(Transaction* t, EngineShard* shard);

  ListDir dir_;

  ShardFFResult ff_result_;

  string key_;
  string value_;
};

BPopper::BPopper(ListDir dir) : dir_(dir) {
}

OpStatus BPopper::Run(Transaction* t, unsigned msec) {
  using time_point = Transaction::time_point;

  time_point tp =
      msec ? chrono::steady_clock::now() + chrono::milliseconds(msec) : time_point::max();
  bool is_multi = t->IsMulti();
  if (!is_multi) {
    t->Schedule();
  }

  auto* stats = ServerState::tl_connection_stats();

  OpResult<ShardFFResult> result = FindFirst(t);

  if (result.status() == OpStatus::KEY_NOTFOUND) {
    if (is_multi) {
      // close transaction and return.
      auto cb = [](Transaction* t, EngineShard* shard) { return OpStatus::OK; };
      t->Execute(std::move(cb), true);

      return OpStatus::TIMED_OUT;
    }

    // Block
    ++stats->num_blocked_clients;
    bool wait_succeeded = t->WaitOnWatch(tp);
    --stats->num_blocked_clients;

    if (!wait_succeeded)
      return OpStatus::TIMED_OUT;

    // Now we have something for sure.
    result = FindFirst(t);  // retry - must find something.
  }

  if (!result) {
    t->UnregisterWatch();
    return result.status();
  }

  VLOG(1) << "Popping an element";
  ff_result_ = move(result.value());

  auto cb = [this](Transaction* t, EngineShard* shard) { return Pop(t, shard); };
  t->Execute(std::move(cb), true);

  return OpStatus::OK;
}

OpStatus BPopper::Pop(Transaction* t, EngineShard* shard) {
  if (shard->shard_id() == ff_result_.sid) {
    ff_result_.key.GetString(&key_);
    auto& db_slice = shard->db_slice();
    auto it_res = db_slice.Find(t->db_index(), key_, OBJ_LIST);
    CHECK(it_res);  // must exist and must be ok.
    PrimeIterator it = *it_res;
    quicklist* ql = GetQL(it->second);

    db_slice.PreUpdate(t->db_index(), it);
    value_ = ListPop(dir_, ql);
    db_slice.PostUpdate(t->db_index(), it);
    if (quicklistCount(ql) == 0) {
      CHECK(shard->db_slice().Del(t->db_index(), it));
    }
  }

  return OpStatus::OK;
}

OpResult<string> OpRPopLPushSingleShard(const OpArgs& op_args, string_view src, string_view dest) {
  auto& db_slice = op_args.shard->db_slice();
  auto src_res = db_slice.Find(op_args.db_ind, src, OBJ_LIST);
  if (!src_res)
    return src_res.status();

  PrimeIterator src_it = *src_res;
  quicklist* src_ql = GetQL(src_it->second);

  if (src == dest) {  // simple case.
    db_slice.PreUpdate(op_args.db_ind, src_it);
    string val = ListPop(ListDir::RIGHT, src_ql);

    quicklistPushHead(src_ql, val.data(), val.size());
    db_slice.PostUpdate(op_args.db_ind, src_it);

    return val;
  }

  quicklist* dest_ql = nullptr;
  pair<PrimeIterator, bool> res;
  try {
    res = db_slice.AddOrFind(op_args.db_ind, dest);
  } catch (bad_alloc&) {
    return OpStatus::OUT_OF_MEMORY;
  }

  PrimeIterator& dest_it = res.first;

  if (res.second) {
    robj* obj = createQuicklistObject();
    dest_ql = (quicklist*)obj->ptr;
    quicklistSetOptions(dest_ql, FLAGS_list_max_listpack_size, FLAGS_list_compress_depth);
    dest_it->second.ImportRObj(obj);

    // Insertion of dest could invalidate src_it. Find it again.
    src_it = db_slice.GetTables(op_args.db_ind).first->Find(src);
  } else {
    if (dest_it->second.ObjType() != OBJ_LIST)
      return OpStatus::WRONG_TYPE;

    dest_ql = GetQL(dest_it->second);
    db_slice.PreUpdate(op_args.db_ind, dest_it);
  }

  db_slice.PreUpdate(op_args.db_ind, src_it);

  string val = ListPop(ListDir::RIGHT, src_ql);
  quicklistPushHead(dest_ql, val.data(), val.size());

  db_slice.PostUpdate(op_args.db_ind, src_it);
  db_slice.PostUpdate(op_args.db_ind, dest_it);

  if (quicklistCount(src_ql) == 0) {
    CHECK(db_slice.Del(op_args.db_ind, src_it));
  }

  return val;
}

// Read-only peek operation that determines wether the list exists and optionally
// returns the first from right value without popping it from the list.
OpResult<string> RPeek(const OpArgs& op_args, string_view key, bool fetch) {
  auto it_res = op_args.shard->db_slice().Find(op_args.db_ind, key, OBJ_LIST);
  if (!it_res) {
    return it_res.status();
  }

  if (!fetch)
    return OpStatus::OK;

  quicklist* ql = GetQL(it_res.value()->second);
  quicklistEntry entry = QLEntry();
  quicklistIter* iter = quicklistGetIterator(ql, AL_START_TAIL);
  CHECK(quicklistNext(iter, &entry));
  quicklistReleaseIterator(iter);

  if (entry.value)
    return string(reinterpret_cast<char*>(entry.value), entry.sz);
  else
    return absl::StrCat(entry.longval);
}

OpResult<uint32_t> OpPush(const OpArgs& op_args, std::string_view key, ListDir dir,
                          bool skip_notexist, absl::Span<std::string_view> vals) {
  EngineShard* es = op_args.shard;
  PrimeIterator it;
  bool new_key = false;

  if (skip_notexist) {
    auto it_res = es->db_slice().Find(op_args.db_ind, key, OBJ_LIST);
    if (!it_res)
      return it_res.status();
    it = *it_res;
  } else {
    try {
      tie(it, new_key) = es->db_slice().AddOrFind(op_args.db_ind, key);
    } catch (bad_alloc&) {
      return OpStatus::OUT_OF_MEMORY;
    }
  }

  quicklist* ql = nullptr;

  if (new_key) {
    robj* o = createQuicklistObject();
    ql = (quicklist*)o->ptr;
    quicklistSetOptions(ql, FLAGS_list_max_listpack_size, FLAGS_list_compress_depth);
    it->second.ImportRObj(o);
  } else {
    if (it->second.ObjType() != OBJ_LIST)
      return OpStatus::WRONG_TYPE;
    es->db_slice().PreUpdate(op_args.db_ind, it);
    ql = GetQL(it->second);
  }

  // Left push is LIST_HEAD.
  int pos = (dir == ListDir::LEFT) ? QUICKLIST_HEAD : QUICKLIST_TAIL;

  for (auto v : vals) {
    es->tmp_str1 = sdscpylen(es->tmp_str1, v.data(), v.size());
    quicklistPush(ql, es->tmp_str1, sdslen(es->tmp_str1), pos);
  }

  if (new_key) {
    if (es->blocking_controller()) {
      string tmp;
      string_view key = it->first.GetSlice(&tmp);
      es->blocking_controller()->AwakeWatched(op_args.db_ind, key);
    }
  } else {
    es->db_slice().PostUpdate(op_args.db_ind, it);
  }

  return quicklistCount(ql);
}

OpResult<StringVec> OpPop(const OpArgs& op_args, string_view key, ListDir dir, uint32_t count,
                          bool return_results) {
  auto& db_slice = op_args.shard->db_slice();
  OpResult<PrimeIterator> it_res = db_slice.Find(op_args.db_ind, key, OBJ_LIST);
  if (!it_res)
    return it_res.status();

  PrimeIterator it = *it_res;
  quicklist* ql = GetQL(it->second);
  db_slice.PreUpdate(op_args.db_ind, it);

  StringVec res;
  if (quicklistCount(ql) < count) {
    count = quicklistCount(ql);
  }
  res.reserve(count);

  if (return_results) {
    for (unsigned i = 0; i < count; ++i) {
      res.push_back(ListPop(dir, ql));
    }
  } else {
    for (unsigned i = 0; i < count; ++i) {
      ListPop(dir, ql);
    }
  }

  db_slice.PostUpdate(op_args.db_ind, it);

  if (quicklistCount(ql) == 0) {
    CHECK(db_slice.Del(op_args.db_ind, it));
  }

  return res;
}

}  // namespace

void ListFamily::LPush(CmdArgList args, ConnectionContext* cntx) {
  return PushGeneric(ListDir::LEFT, false, std::move(args), cntx);
}

void ListFamily::LPushX(CmdArgList args, ConnectionContext* cntx) {
  return PushGeneric(ListDir::LEFT, true, std::move(args), cntx);
}

void ListFamily::LPop(CmdArgList args, ConnectionContext* cntx) {
  return PopGeneric(ListDir::LEFT, std::move(args), cntx);
}

void ListFamily::RPush(CmdArgList args, ConnectionContext* cntx) {
  return PushGeneric(ListDir::RIGHT, false, std::move(args), cntx);
}

void ListFamily::RPushX(CmdArgList args, ConnectionContext* cntx) {
  return PushGeneric(ListDir::RIGHT, true, std::move(args), cntx);
}

void ListFamily::RPop(CmdArgList args, ConnectionContext* cntx) {
  return PopGeneric(ListDir::RIGHT, std::move(args), cntx);
}

void ListFamily::RPopLPush(CmdArgList args, ConnectionContext* cntx) {
  string_view src = ArgS(args, 1);
  string_view dest = ArgS(args, 2);

  OpResult<string> result;

  if (cntx->transaction->unique_shard_cnt() == 1) {
    auto cb = [&](Transaction* t, EngineShard* shard) {
      return OpRPopLPushSingleShard(OpArgs{shard, t->db_index()}, src, dest);
    };

    result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  } else {
    CHECK_EQ(2u, cntx->transaction->unique_shard_cnt());

    OpResult<string> find_res[2];

    // Transaction is comprised of 2 hops:
    // 1 - check for entries existence, their types and if possible -
    //     read the value we may rpop from the source list.
    // 2.  If everything is ok, rpop from source and lpush the peeked value into
    //     the destination.
    //
    cntx->transaction->Schedule();
    auto cb = [&](Transaction* t, EngineShard* shard) {
      auto args = t->ShardArgsInShard(shard->shard_id());
      DCHECK_EQ(1u, args.size());
      bool is_dest = args.front() == dest;
      find_res[is_dest] = RPeek(OpArgs{shard, t->db_index()}, args.front(), !is_dest);
      return OpStatus::OK;
    };

    cntx->transaction->Execute(move(cb), false);

    if (!find_res[0] || find_res[1].status() == OpStatus::WRONG_TYPE) {
      auto cb = [&](Transaction* t, EngineShard* shard) { return OpStatus::OK; };
      cntx->transaction->Execute(move(cb), true);
      result = find_res[0] ? find_res[1] : find_res[0];
    } else {
      // Everything is ok, lets proceed with the mutations.
      auto cb = [&](Transaction* t, EngineShard* shard) {
        auto args = t->ShardArgsInShard(shard->shard_id());
        bool is_dest = args.front() == dest;
        OpArgs op_args{shard, t->db_index()};

        if (is_dest) {
          string_view val{find_res[0].value()};
          absl::Span<string_view> span{&val, 1};
          OpPush(op_args, args.front(), ListDir::LEFT, false, span);
        } else {
          OpPop(op_args, args.front(), ListDir::RIGHT, 1, false);
        }
        return OpStatus::OK;
      };
      cntx->transaction->Execute(move(cb), true);
      result = std::move(find_res[0].value());
    }
  }

  if (result) {
    return (*cntx)->SendBulkString(*result);
  }

  switch (result.status()) {
    case OpStatus::KEY_NOTFOUND:
      (*cntx)->SendNull();
      break;

    default:
      (*cntx)->SendError(result.status());
      break;
  }
}

void ListFamily::LLen(CmdArgList args, ConnectionContext* cntx) {
  auto key = ArgS(args, 1);
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpLen(OpArgs{shard, t->db_index()}, key);
  };
  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result) {
    (*cntx)->SendLong(result.value());
  } else if (result.status() == OpStatus::KEY_NOTFOUND) {
    (*cntx)->SendLong(0);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void ListFamily::LIndex(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);
  std::string_view index_str = ArgS(args, 2);
  int32_t index;
  if (!absl::SimpleAtoi(index_str, &index)) {
    (*cntx)->SendError(kInvalidIntErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpIndex(OpArgs{shard, t->db_index()}, key, index);
  };

  OpResult<string> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result) {
    (*cntx)->SendBulkString(result.value());
  } else if (result.status() == OpStatus::WRONG_TYPE) {
    (*cntx)->SendError(result.status());
  } else {
    (*cntx)->SendNull();
  }
}

/* LINSERT <key> (BEFORE|AFTER) <pivot> <element> */
void ListFamily::LInsert(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  string_view param = ArgS(args, 2);
  string_view pivot = ArgS(args, 3);
  string_view elem = ArgS(args, 4);
  int where;

  ToUpper(&args[2]);
  if (param == "AFTER") {
    where = LIST_TAIL;
  } else if (param == "BEFORE") {
    where = LIST_HEAD;
  } else {
    return (*cntx)->SendError(kSyntaxErr);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpInsert(OpArgs{shard, t->db_index()}, key, pivot, elem, where);
  };

  OpResult<int> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result) {
    return (*cntx)->SendLong(result.value());
  }

  (*cntx)->SendError(result.status());
}

void ListFamily::LTrim(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  string_view s_str = ArgS(args, 2);
  string_view e_str = ArgS(args, 3);
  int32_t start, end;

  if (!absl::SimpleAtoi(s_str, &start) || !absl::SimpleAtoi(e_str, &end)) {
    (*cntx)->SendError(kInvalidIntErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpTrim(OpArgs{shard, t->db_index()}, key, start, end);
  };
  cntx->transaction->ScheduleSingleHop(std::move(cb));
  (*cntx)->SendOk();
}

void ListFamily::LRange(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);
  std::string_view s_str = ArgS(args, 2);
  std::string_view e_str = ArgS(args, 3);
  int32_t start, end;

  if (!absl::SimpleAtoi(s_str, &start) || !absl::SimpleAtoi(e_str, &end)) {
    (*cntx)->SendError(kInvalidIntErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpRange(OpArgs{shard, t->db_index()}, key, start, end);
  };

  auto res = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (!res && res.status() != OpStatus::KEY_NOTFOUND) {
    return (*cntx)->SendError(res.status());
  }

  (*cntx)->SendStringArr(*res);
}

// lrem key 5 foo, will remove foo elements from the list if exists at most 5 times.
void ListFamily::LRem(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);
  std::string_view index_str = ArgS(args, 2);
  std::string_view elem = ArgS(args, 3);
  int32_t count;

  if (!absl::SimpleAtoi(index_str, &count)) {
    (*cntx)->SendError(kInvalidIntErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpRem(OpArgs{shard, t->db_index()}, key, elem, count);
  };
  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result) {
    (*cntx)->SendLong(result.value());
  } else {
    (*cntx)->SendLong(0);
  }
}

void ListFamily::LSet(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);
  std::string_view index_str = ArgS(args, 2);
  std::string_view elem = ArgS(args, 3);
  int32_t count;

  if (!absl::SimpleAtoi(index_str, &count)) {
    (*cntx)->SendError(kInvalidIntErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpSet(OpArgs{shard, t->db_index()}, key, elem, count);
  };
  OpResult<void> result = cntx->transaction->ScheduleSingleHop(std::move(cb));
  if (result) {
    (*cntx)->SendOk();
  } else {
    (*cntx)->SendError(result.status());
  }
}

void ListFamily::BLPop(CmdArgList args, ConnectionContext* cntx) {
  BPopGeneric(ListDir::LEFT, std::move(args), cntx);
}

void ListFamily::BRPop(CmdArgList args, ConnectionContext* cntx) {
  BPopGeneric(ListDir::RIGHT, std::move(args), cntx);
}

void ListFamily::BPopGeneric(ListDir dir, CmdArgList args, ConnectionContext* cntx) {
  DCHECK_GE(args.size(), 3u);

  float timeout;
  auto timeout_str = ArgS(args, args.size() - 1);
  if (!absl::SimpleAtof(timeout_str, &timeout)) {
    return (*cntx)->SendError("timeout is not a float or out of range");
  }
  if (timeout < 0) {
    return (*cntx)->SendError("timeout is negative");
  }
  VLOG(1) << "BLPop start " << timeout;

  Transaction* transaction = cntx->transaction;
  BPopper popper(dir);
  OpStatus result = popper.Run(transaction, unsigned(timeout * 1000));

  if (result == OpStatus::OK) {
    auto res = popper.result();

    VLOG(1) << "BLPop returned from " << res.first;  // key.

    std::string_view str_arr[2] = {res.first, res.second};

    return (*cntx)->SendStringArr(str_arr);
  }

  switch (result) {
    case OpStatus::WRONG_TYPE:
      return (*cntx)->SendError(kWrongTypeErr);
    case OpStatus::TIMED_OUT:
      return (*cntx)->SendNullArray();
    default:
      LOG(ERROR) << "Unexpected error " << result;
  }
  return (*cntx)->SendNullArray();
}

void ListFamily::PushGeneric(ListDir dir, bool skip_notexists, CmdArgList args,
                             ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);
  vector<std::string_view> vals(args.size() - 2);
  for (size_t i = 2; i < args.size(); ++i) {
    vals[i - 2] = ArgS(args, i);
  }
  absl::Span<std::string_view> span{vals.data(), vals.size()};
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpPush(OpArgs{shard, t->db_index()}, key, dir, skip_notexists, span);
  };

  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result) {
    return (*cntx)->SendLong(result.value());
  }

  return (*cntx)->SendError(result.status());
}

void ListFamily::PopGeneric(ListDir dir, CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  int32_t count = 1;
  bool return_arr = false;

  if (args.size() > 2) {
    if (args.size() > 3) {
      ToLower(&args[0]);
      return (*cntx)->SendError(WrongNumArgsError(ArgS(args, 0)));
    }

    string_view count_s = ArgS(args, 2);
    if (!absl::SimpleAtoi(count_s, &count)) {
      return (*cntx)->SendError(kInvalidIntErr);
    }

    if (count < 0) {
      return (*cntx)->SendError(kUintErr);
    }
    return_arr = true;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpPop(OpArgs{shard, t->db_index()}, key, dir, count, true);
  };

  OpResult<StringVec> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));

  switch (result.status()) {
    case OpStatus::KEY_NOTFOUND:
      return (*cntx)->SendNull();
    case OpStatus::WRONG_TYPE:
      return (*cntx)->SendError(kWrongTypeErr);
    default:;
  }

  if (return_arr) {
    if (result->empty()) {
      (*cntx)->SendNullArray();
    } else {
      (*cntx)->StartArray(result->size());
      for (const auto& k : *result) {
        (*cntx)->SendBulkString(k);
      }
    }
  } else {
    DCHECK_EQ(1u, result->size());
    (*cntx)->SendBulkString(result->front());
  }
}

OpResult<uint32_t> ListFamily::OpLen(const OpArgs& op_args, std::string_view key) {
  auto res = op_args.shard->db_slice().Find(op_args.db_ind, key, OBJ_LIST);
  if (!res)
    return res.status();

  quicklist* ql = GetQL(res.value()->second);

  return quicklistCount(ql);
}

OpResult<string> ListFamily::OpIndex(const OpArgs& op_args, std::string_view key, long index) {
  auto res = op_args.shard->db_slice().Find(op_args.db_ind, key, OBJ_LIST);
  if (!res)
    return res.status();
  quicklist* ql = GetQL(res.value()->second);
  quicklistEntry entry = QLEntry();
  quicklistIter* iter = quicklistGetIteratorAtIdx(ql, AL_START_TAIL, index);
  if (!iter)
    return OpStatus::KEY_NOTFOUND;

  quicklistNext(iter, &entry);
  string str;

  if (entry.value) {
    str.assign(reinterpret_cast<char*>(entry.value), entry.sz);
  } else {
    str = absl::StrCat(entry.longval);
  }
  quicklistReleaseIterator(iter);

  return str;
}

OpResult<int> ListFamily::OpInsert(const OpArgs& op_args, string_view key, string_view pivot,
                                   string_view elem, int insert_param) {
  auto& db_slice = op_args.shard->db_slice();
  auto it_res = db_slice.Find(op_args.db_ind, key, OBJ_LIST);
  if (!it_res)
    return it_res.status();

  quicklist* ql = GetQL(it_res.value()->second);
  quicklistEntry entry = QLEntry();
  quicklistIter* qiter = quicklistGetIterator(ql, AL_START_HEAD);
  bool found = false;

  while (quicklistNext(qiter, &entry)) {
    if (ElemCompare(entry, pivot)) {
      found = true;
      break;
    }
  }

  int res = -1;
  if (found) {
    db_slice.PreUpdate(op_args.db_ind, *it_res);
    if (insert_param == LIST_TAIL) {
      quicklistInsertAfter(qiter, &entry, elem.data(), elem.size());
    } else {
      DCHECK_EQ(LIST_HEAD, insert_param);
      quicklistInsertBefore(qiter, &entry, elem.data(), elem.size());
    }
    db_slice.PostUpdate(op_args.db_ind, *it_res);
    res = quicklistCount(ql);
  }
  quicklistReleaseIterator(qiter);
  return res;
}

OpResult<uint32_t> ListFamily::OpRem(const OpArgs& op_args, string_view key, string_view elem,
                                     long count) {
  DCHECK(!elem.empty());
  auto& db_slice = op_args.shard->db_slice();
  auto it_res = db_slice.Find(op_args.db_ind, key, OBJ_LIST);
  if (!it_res)
    return it_res.status();

  PrimeIterator it = *it_res;
  quicklist* ql = GetQL(it->second);

  int iter_direction = AL_START_HEAD;
  long long index = 0;
  if (count < 0) {
    count = -count;
    iter_direction = AL_START_TAIL;
    index = -1;
  }

  quicklistIter* qiter = quicklistGetIteratorAtIdx(ql, iter_direction, index);
  quicklistEntry entry;
  unsigned removed = 0;
  const uint8_t* elem_ptr = reinterpret_cast<const uint8_t*>(elem.data());

  db_slice.PreUpdate(op_args.db_ind, it);
  while (quicklistNext(qiter, &entry)) {
    if (quicklistCompare(&entry, elem_ptr, elem.size())) {
      quicklistDelEntry(qiter, &entry);
      removed++;
      if (count && removed == count)
        break;
    }
  }
  db_slice.PostUpdate(op_args.db_ind, it);

  quicklistReleaseIterator(qiter);

  if (quicklistCount(ql) == 0) {
    CHECK(db_slice.Del(op_args.db_ind, it));
  }

  return removed;
}

OpStatus ListFamily::OpSet(const OpArgs& op_args, string_view key, string_view elem, long index) {
  DCHECK(!elem.empty());
  auto& db_slice = op_args.shard->db_slice();
  auto it_res = db_slice.Find(op_args.db_ind, key, OBJ_LIST);
  if (!it_res)
    return it_res.status();

  PrimeIterator it = *it_res;
  quicklist* ql = GetQL(it->second);

  db_slice.PreUpdate(op_args.db_ind, it);
  int replaced = quicklistReplaceAtIndex(ql, index, elem.data(), elem.size());
  db_slice.PostUpdate(op_args.db_ind, it);

  if (!replaced) {
    return OpStatus::OUT_OF_RANGE;
  }
  return OpStatus::OK;
}

OpStatus ListFamily::OpTrim(const OpArgs& op_args, string_view key, long start, long end) {
  auto& db_slice = op_args.shard->db_slice();
  auto it_res = db_slice.Find(op_args.db_ind, key, OBJ_LIST);
  if (!it_res)
    return it_res.status();

  PrimeIterator it = *it_res;
  quicklist* ql = GetQL(it->second);
  long llen = quicklistCount(ql);

  /* convert negative indexes */
  if (start < 0)
    start = llen + start;
  if (end < 0)
    end = llen + end;
  if (start < 0)
    start = 0;

  long ltrim, rtrim;

  /* Invariant: start >= 0, so this test will be true when end < 0.
   * The range is empty when start > end or start >= length. */
  if (start > end || start >= llen) {
    /* Out of range start or start > end result in empty list */
    ltrim = llen;
    rtrim = 0;
  } else {
    if (end >= llen)
      end = llen - 1;
    ltrim = start;
    rtrim = llen - end - 1;
  }

  db_slice.PreUpdate(op_args.db_ind, it);
  quicklistDelRange(ql, 0, ltrim);
  quicklistDelRange(ql, -rtrim, rtrim);
  db_slice.PostUpdate(op_args.db_ind, it);

  if (quicklistCount(ql) == 0) {
    CHECK(db_slice.Del(op_args.db_ind, it));
  }
  return OpStatus::OK;
}

OpResult<StringVec> ListFamily::OpRange(const OpArgs& op_args, std::string_view key, long start,
                                        long end) {
  auto res = op_args.shard->db_slice().Find(op_args.db_ind, key, OBJ_LIST);
  if (!res)
    return res.status();

  quicklist* ql = GetQL(res.value()->second);
  long llen = quicklistCount(ql);

  /* convert negative indexes */
  if (start < 0)
    start = llen + start;
  if (end < 0)
    end = llen + end;
  if (start < 0)
    start = 0;

  /* Invariant: start >= 0, so this test will be true when end < 0.
   * The range is empty when start > end or start >= length. */
  if (start > end || start >= llen) {
    /* Out of range start or start > end result in empty list */
    return StringVec{};
  }

  if (end >= llen)
    end = llen - 1;

  unsigned lrange = end - start + 1;
  quicklistIter* qiter = quicklistGetIteratorAtIdx(ql, AL_START_HEAD, start);
  quicklistEntry entry = QLEntry();
  StringVec str_vec;

  unsigned cnt = 0;
  while (cnt < lrange && quicklistNext(qiter, &entry)) {
    if (entry.value)
      str_vec.emplace_back(reinterpret_cast<char*>(entry.value), entry.sz);
    else
      str_vec.push_back(absl::StrCat(entry.longval));
    ++cnt;
  }
  quicklistReleaseIterator(qiter);

  return str_vec;
}

using CI = CommandId;

#define HFUNC(x) SetHandler(&ListFamily::x)

void ListFamily::Register(CommandRegistry* registry) {
  *registry << CI{"LPUSH", CO::WRITE | CO::FAST | CO::DENYOOM, -3, 1, 1, 1}.HFUNC(LPush)
            << CI{"LPUSHX", CO::WRITE | CO::FAST | CO::DENYOOM, -3, 1, 1, 1}.HFUNC(LPushX)
            << CI{"LPOP", CO::WRITE | CO::FAST | CO::DENYOOM, -2, 1, 1, 1}.HFUNC(LPop)
            << CI{"RPUSH", CO::WRITE | CO::FAST | CO::DENYOOM, -3, 1, 1, 1}.HFUNC(RPush)
            << CI{"RPUSHX", CO::WRITE | CO::FAST | CO::DENYOOM, -3, 1, 1, 1}.HFUNC(RPushX)
            << CI{"RPOP", CO::WRITE | CO::FAST | CO::DENYOOM, -2, 1, 1, 1}.HFUNC(RPop)
            << CI{"RPOPLPUSH", CO::WRITE | CO::FAST | CO::DENYOOM, 3, 1, 2, 1}.HFUNC(RPopLPush)
            << CI{"BLPOP", CO::WRITE | CO::NOSCRIPT | CO::BLOCKING, -3, 1, -2, 1}.HFUNC(BLPop)
            << CI{"BRPOP", CO::WRITE | CO::NOSCRIPT | CO::BLOCKING, -3, 1, -2, 1}.HFUNC(BRPop)
            << CI{"LLEN", CO::READONLY | CO::FAST, 2, 1, 1, 1}.HFUNC(LLen)
            << CI{"LINDEX", CO::READONLY, 3, 1, 1, 1}.HFUNC(LIndex)
            << CI{"LINSERT", CO::WRITE, 5, 1, 1, 1}.HFUNC(LInsert)
            << CI{"LRANGE", CO::READONLY, 4, 1, 1, 1}.HFUNC(LRange)
            << CI{"LSET", CO::WRITE | CO::DENYOOM, 4, 1, 1, 1}.HFUNC(LSet)
            << CI{"LTRIM", CO::WRITE, 4, 1, 1, 1}.HFUNC(LTrim)
            << CI{"LREM", CO::WRITE, 4, 1, 1, 1}.HFUNC(LRem);
}

}  // namespace dfly
