// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/data-stream-mgr.h"

#include <iostream>
#include <boost/functional/hash.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/thread.hpp>

#include "runtime/data-stream-recvr.h"
#include "runtime/raw-value.inline.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "service/data_stream_service.pb.h"
#include "util/debug-util.h"
#include "util/periodic-counter-updater.h"
#include "util/runtime-profile-counters.h"
#include "util/uid-util.h"

#include "kudu/rpc/rpc_context.h"

#include "common/names.h"

using std::boolalpha;
using kudu::rpc::ErrorStatusPB;
using kudu::rpc::RpcContext;

DEFINE_int32(datastream_sender_timeout_ms, 120000, "(Advanced) The time, in ms, that can "
    "elapse before a plan fragment will time-out trying to send a row batch.");

/// This parameter controls the minimum amount of time a closed stream ID will stay in
/// closed_stream_cache_ before it is evicted. It needs to be set sufficiently high that
/// it will outlive all the calls to FindRecvr() for that stream ID, to distinguish
/// between was-here-but-now-gone and never-here states for the receiver. If the stream ID
/// expires before a call to FindRecvr(), the sender will see an error which will lead to
/// query cancellation. Setting this value higher will increase the size of the stream
/// cache (which is roughly 48 bytes per receiver).
/// TODO: We don't need millisecond precision here.
const int32_t STREAM_EXPIRATION_TIME_MS = 300 * 1000;

namespace impala {

DataStreamMgr::DataStreamMgr(MetricGroup* metrics)
    : deserialize_pool_("data-stream-mgr", "sender-response", 4, 10000,
        [this](int thread_id, DeserializeWorkItem&& workitem) {
          bool unused = false;
          shared_ptr<DataStreamRecvr> recvr = this->FindRecvr(
              workitem.fragment_instance_id, workitem.ctx->request->dest_node_id(), &unused);
          if (recvr) {
            recvr->AddBatch(move(workitem.ctx));
          } else {
            workitem.ctx->context->RespondSuccess();
          }
        }) {
  MetricGroup* dsm_metrics = metrics->GetOrCreateChildGroup("datastream-manager");
  num_senders_waiting_ =
      dsm_metrics->AddGauge<int64_t>("senders-blocked-on-recvr-creation", 0L);
  total_senders_waited_ =
      dsm_metrics->AddCounter<int64_t>("total-senders-blocked-on-recvr-creation", 0L);
  num_senders_timedout_ = dsm_metrics->AddCounter<int64_t>(
      "total-senders-timedout-waiting-for-recvr-creation", 0L);
  maintenance_thread_.reset(
      new Thread("data-stream-mgr", "maintenance", [this](){ this->Maintenance(); }));
}

void DataStreamMgr::EnqueueRowBatch(DeserializeWorkItem&& payload) {
  deserialize_pool_.Offer(move(payload));
}

inline uint32_t DataStreamMgr::GetHashValue(
    const TUniqueId& fragment_instance_id, PlanNodeId node_id) {
  uint32_t value = RawValue::GetHashValue(&fragment_instance_id.lo, TYPE_BIGINT, 0);
  value = RawValue::GetHashValue(&fragment_instance_id.hi, TYPE_BIGINT, value);
  value = RawValue::GetHashValue(&node_id, TYPE_INT, value);
  return value;
}

shared_ptr<DataStreamRecvr> DataStreamMgr::CreateRecvr(RuntimeState* state,
    const RowDescriptor* row_desc, const TUniqueId& fragment_instance_id,
    PlanNodeId dest_node_id, int num_senders, int buffer_size, RuntimeProfile* profile,
    bool is_merging) {
  DCHECK(profile != nullptr);
  VLOG_FILE << "creating receiver for fragment="
            << fragment_instance_id << ", node=" << dest_node_id;
  shared_ptr<DataStreamRecvr> recvr(
      new DataStreamRecvr(this, state->instance_mem_tracker(), row_desc,
          fragment_instance_id, dest_node_id, num_senders, is_merging, buffer_size,
          profile));
  size_t hash_value = GetHashValue(fragment_instance_id, dest_node_id);
  EarlySendersList waiters;
  {
    lock_guard<mutex> l(lock_);
    fragment_recvr_set_.insert(make_pair(fragment_instance_id, dest_node_id));
    receiver_map_.insert(make_pair(hash_value, recvr));

    EarlySendersMap::iterator it =
        early_senders_.find(make_pair(fragment_instance_id, dest_node_id));
    if (it == early_senders_.end()) return recvr;

    waiters = move(it->second);
    early_senders_.erase(it);
  }

  for (unique_ptr<TransmitDataCtx>& ctx: waiters.waiting_senders) {
    EnqueueRowBatch({recvr->fragment_instance_id(), move(ctx)});
    num_senders_waiting_->Increment(-1);
  }
  for (int32_t sender_id: waiters.closing_senders) recvr->RemoveSender(sender_id);

  return recvr;
}

shared_ptr<DataStreamRecvr> DataStreamMgr::FindRecvr(
    const TUniqueId& fragment_instance_id, PlanNodeId node_id,
    bool* already_unregistered) {
  VLOG_ROW << "looking up fragment_instance_id=" << fragment_instance_id
           << ", node=" << node_id;
  *already_unregistered = false;
  RecvrId recvr_id = make_pair(fragment_instance_id, node_id);
  if (closed_stream_cache_.find(recvr_id) != closed_stream_cache_.end()) {
    *already_unregistered = true;
    return shared_ptr<DataStreamRecvr>();
  }

  size_t hash_value = GetHashValue(fragment_instance_id, node_id);
  pair<RecvrMap::iterator, RecvrMap::iterator> range =
      receiver_map_.equal_range(hash_value);
  while (range.first != range.second) {
    shared_ptr<DataStreamRecvr> recvr = range.first->second;
    if (recvr->fragment_instance_id() == fragment_instance_id
        && recvr->dest_node_id() == node_id) {
      return recvr;
    }
    ++range.first;
  }
  return shared_ptr<DataStreamRecvr>();
}

void DataStreamMgr::AddData(const TUniqueId& fragment_instance_id,
    unique_ptr<TransmitDataCtx>&& payload) {
  VLOG_ROW << "AddData(): fragment_instance_id=" << fragment_instance_id
           << " node=" << payload->request->dest_node_id() << " size="
           << payload->proto_batch.GetSize();
  bool already_unregistered = false;
  shared_ptr<DataStreamRecvr> recvr;
  {
    lock_guard<mutex> l(lock_);
    recvr = FindRecvr(
        fragment_instance_id, payload->request->dest_node_id(), &already_unregistered);
    // If no receiver found, but not in the closed stream cache, best guess is that it is
    // still preparing, so add payload to per-receiver list.
    if (!already_unregistered && !recvr) {
      num_senders_waiting_->Increment(1);
      total_senders_waited_->Increment(1);
      RecvrId recvr_id = make_pair(fragment_instance_id, payload->request->dest_node_id());
      early_senders_[recvr_id].waiting_senders.emplace_back(move(payload));
      return;
    }
  }
  if (already_unregistered) {
    // The receiver may remove itself from the receiver map via DeregisterRecvr() at any
    // time without considering the remaining number of senders.  As a consequence,
    // FindRecvr() may return nullptr even though the receiver was once present. We
    // detect this case by checking already_unregistered - if true then the receiver was
    // already closed deliberately, and there's no unexpected error here.
    Status(TErrorCode::DATASTREAM_RECVR_ALREADY_GONE).ToProto(
        payload->response->mutable_status());
    payload->context->RespondSuccess();
    return;
  }

  // Don't hold lock.
  DCHECK(recvr.get() != nullptr);
  recvr->AddBatch(move(payload));
}

Status DataStreamMgr::CloseSender(const TUniqueId& fragment_instance_id,
    PlanNodeId dest_node_id, int sender_id) {
  VLOG_FILE << "CloseSender(): fragment_instance_id=" << fragment_instance_id
            << ", node=" << dest_node_id;
  shared_ptr<DataStreamRecvr> recvr;
  {
    lock_guard<mutex> l(lock_);
    bool already_unregistered;
    recvr = FindRecvr(fragment_instance_id, dest_node_id, &already_unregistered);
    // If no receiver found, but not in the closed stream cache, still need to make sure
    // that the close operation is performed so add to per-recvr list of pending closes.
    if (!already_unregistered && !recvr) {
      RecvrId recvr_id = make_pair(fragment_instance_id, dest_node_id);
      early_senders_[recvr_id].closing_senders.emplace_back(sender_id);
      return Status::OK();
    }
  }

  if (recvr) recvr->RemoveSender(sender_id);

  {
    // Remove any closed streams that have been in the cache for more than
    // STREAM_EXPIRATION_TIME_MS.
    lock_guard<mutex> l(lock_);
    ClosedStreamMap::iterator it = closed_stream_expirations_.begin();
    int64_t now = MonotonicMillis();
    int32_t before = closed_stream_cache_.size();
    while (it != closed_stream_expirations_.end() && it->first < now) {
      closed_stream_cache_.erase(it->second);
      closed_stream_expirations_.erase(it++);
    }
    DCHECK_EQ(closed_stream_cache_.size(), closed_stream_expirations_.size());
    int32_t after = closed_stream_cache_.size();
    if (before != after) {
      VLOG_QUERY << "Reduced stream ID cache from " << before << " items, to " << after
                 << ", eviction took: "
                 << PrettyPrinter::Print(MonotonicMillis() - now, TUnit::TIME_MS);
    }
  }
  return Status::OK();
}

Status DataStreamMgr::DeregisterRecvr(
    const TUniqueId& fragment_instance_id, PlanNodeId node_id) {
  VLOG_QUERY << "DeregisterRecvr(): fragment_instance_id=" << fragment_instance_id
             << ", node=" << node_id;
  size_t hash_value = GetHashValue(fragment_instance_id, node_id);
  lock_guard<mutex> l(lock_);
  pair<RecvrMap::iterator, RecvrMap::iterator> range =
      receiver_map_.equal_range(hash_value);
  while (range.first != range.second) {
    const shared_ptr<DataStreamRecvr>& recvr = range.first->second;
    if (recvr->fragment_instance_id() == fragment_instance_id
        && recvr->dest_node_id() == node_id) {
      // Notify concurrent AddData() requests that the stream has been terminated.
      recvr->CancelStream();
      RecvrId recvr_id =
          make_pair(recvr->fragment_instance_id(), recvr->dest_node_id());
      fragment_recvr_set_.erase(recvr_id);
      receiver_map_.erase(range.first);
      closed_stream_expirations_.insert(
          make_pair(MonotonicMillis() + STREAM_EXPIRATION_TIME_MS, recvr_id));
      closed_stream_cache_.insert(recvr_id);
      return Status::OK();
    }
    ++range.first;
  }

  const string msg = Substitute(
      "Unknown row receiver id: fragment_instance_id=$0, node_id=$1",
      PrintId(fragment_instance_id), node_id);
  LOG(ERROR) << msg;
  return Status(msg);
}

void DataStreamMgr::Cancel(const TUniqueId& fragment_instance_id) {
  VLOG_QUERY << "cancelling all streams for fragment=" << fragment_instance_id;
  lock_guard<mutex> l(lock_);
  FragmentRecvrSet::iterator i =
      fragment_recvr_set_.lower_bound(make_pair(fragment_instance_id, 0));
  while (i != fragment_recvr_set_.end() && i->first == fragment_instance_id) {
    bool unused;
    shared_ptr<DataStreamRecvr> recvr = FindRecvr(i->first, i->second, &unused);
    if (recvr) {
      recvr->CancelStream();
    } else {
      // keep going but at least log it
      LOG(ERROR) << Substitute("Cancel(): missing in stream_map: fragment=$0 node=$1",
          PrintId(i->first), i->second);
    }
    ++i;
  }
}

void DataStreamMgr::Maintenance() {
  while (true) {
    // Notify any senders that have been waiting too long for their receiver to
    // appear. Keep lock_ held for only a short amount of time.
    vector<unique_ptr<TransmitDataCtx>> senders;
    {
      int64_t now = MonotonicMillis();
      lock_guard<mutex> l(lock_);
      auto it = early_senders_.begin();
      while (it != early_senders_.end()) {
        if (now - it->second.arrival_time > FLAGS_datastream_sender_timeout_ms) {
          for (auto& s: it->second.waiting_senders) {
            senders.emplace_back(move(s));
          }
          it = early_senders_.erase(it);
        } else {
          ++it;
        }
      }
    }

    for (const auto& ctx: senders) {
      const TransmitDataRequestPb* request = ctx->request;
      TUniqueId finst_id;
      finst_id.__set_lo(request->dest_fragment_instance_id().lo());
      finst_id.__set_hi(request->dest_fragment_instance_id().hi());

      Status(TErrorCode::DATASTREAM_SENDER_TIMEOUT, PrintId(finst_id)).ToProto(
          ctx->response->mutable_status());
      ctx->context->RespondSuccess();
      num_senders_waiting_->Increment(-1);
      num_senders_timedout_->Increment(1);
    }

    bool timed_out = false;
    // Wait for 10s
    shutdown_promise_.Get(10000, &timed_out);
    if (!timed_out) return;
  }
}

DataStreamMgr::~DataStreamMgr() {
  shutdown_promise_.Set(true);
  deserialize_pool_.Shutdown();
  LOG(INFO) << "Waiting for data-stream-mgr maintenance thread...";
  maintenance_thread_->Join();
  LOG(INFO) << "Waiting for response thread pool...";
  deserialize_pool_.Join();
}

}
