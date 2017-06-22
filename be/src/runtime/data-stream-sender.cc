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

#include "runtime/data-stream-sender.h"

#include <condition_variable>
#include <iostream>
#include <memory>

#include "common/logging.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exec/kudu-util.h"
#include "gutil/strings/substitute.h"
#include "kudu/rpc/rpc_controller.h"
#include "rpc/rpc.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/descriptors.h"
#include "runtime/exec-env.h"
#include "runtime/raw-value.inline.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"
#include "service/data_stream_service.pb.h"
#include "service/data_stream_service.proxy.h"
#include "util/aligned-new.h"
#include "util/debug-util.h"
#include "util/network-util.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

using kudu::rpc::RpcController;
using kudu::MonoDelta;
using kudu::Slice;

using std::chrono::system_clock;
using std::chrono::milliseconds;
using std::condition_variable_any;
using std::enable_shared_from_this;
using std::weak_ptr;

DEFINE_int32(rpc_end_datastream_timeout_ms, 10000, "Timeout for datastream end RPCs");

namespace impala {

// A channel sends data asynchronously via calls to Send[Serialized]Batch() to a single
// destination fragment instance.
//
// It has a fixed-capacity buffer and allows the caller either to add rows to that buffer
// individually (AddRow()), or circumvent the buffer altogether and send row batches
// directly (Send[Serialized]Batch()). Either way, there can only be one in-flight RPC at
// any one time (ie, sending will block if the most recent rpc hasn't finished, which
// allows the receiver node to throttle the sender by withholding acks). Serialization of
// a row batch to protobuf can happen concurrently with sending a batch. *Not*
// thread-safe.
//
// The Channel is referred to by shared_ptrs so that any RPC callbacks which refer to the
// channel can safely use a weak_ptr to check for the channel's continued existence.
class DataStreamSender::Channel :
      public CacheLineAligned, public enable_shared_from_this<DataStreamSender::Channel> {
 public:
  // Create channel to send data to particular ipaddress/port/query/node
  // combination. buffer_size is specified in bytes and a soft limit on how much tuple
  // data is getting accumulated before being sent; it only applies when data is added via
  // AddRow() and not sent directly via SendBatch().
  Channel(DataStreamSender* parent, const RowDescriptor* row_desc,
      const TNetworkAddress& destination, const TUniqueId& fragment_instance_id,
      PlanNodeId dest_node_id, int buffer_size)
    : parent_(parent),
      buffer_size_(buffer_size),
      row_desc_(row_desc),
      address_(destination),
      fragment_instance_id_(fragment_instance_id),
      dest_node_id_(dest_node_id),
      num_data_bytes_sent_(0) {
  }

  // Initialize channel. Returns OK if successful, error indication otherwise.
  Status Init();

  // Copies a single row into this channel's output buffer and flushes buffer
  // if it reaches capacity.
  // Returns error status if any of the preceding rpcs failed, OK otherwise.
  Status AddRow(TupleRow* row);

  // Asynchronously sends a row batch. Returns the status of the most recently finished
  // TransmitData rpc (or OK if there wasn't one that hasn't been reported yet).
  Status SendSerializedBatch(const shared_ptr<OutboundProtoRowBatch>& batch);
  Status SendBatch(RowBatch* batch);

  // Waits until the channel is clear to send - that is, any outstanding RPCs have
  // completed - or until the channel is closed or the sender has cancelled. Returns
  // status of last TransmitData() RPC, OK() if no RPCs have been made yet.
  Status WaitForClearChannel();

  // Releases batch_, and any serialized protobuf batches.
  void ReleaseResources();

  // Flushes any buffered row batches and sends the EOS RPC to close the channel.
  Status FlushAndSendEos();

  int64_t num_data_bytes_sent() const { return num_data_bytes_sent_.Load(); }

 private:
  DataStreamSender* parent_;
  int buffer_size_;

  const RowDescriptor* row_desc_;
  TNetworkAddress address_;
  TUniqueId fragment_instance_id_;
  PlanNodeId dest_node_id_;

  // The number of RowBatchPb.data() bytes sent successfully. Updated by RPC completion
  // handler, read asynchronously by parent datastream.
  AtomicInt64 num_data_bytes_sent_;

  // we're accumulating rows into this batch
  scoped_ptr<RowBatch> batch_;

  // Two protobuf batches, one that is for outgoing RPCs, and one that is for serializing
  // a new batch to.
  vector<shared_ptr<OutboundProtoRowBatch>> proto_batches_ =
      {make_shared<OutboundProtoRowBatch>(), make_shared<OutboundProtoRowBatch>()};

  // Index of the protobuf batch in proto_batches_ to use for serialization.
  int proto_batch_idx_ = 0;

  /// Reference to 'this' which must exist for any other shared_ptr references to
  /// correctly return the same shared_ptr, which in turn enables the use of
  /// weak_ptr(self_) in the TransmitDat() RPC completion callback. Set in Init(), and
  /// reset in ReleaseResources().
  shared_ptr<DataStreamSender::Channel> self_;

  // Lock with rpc_done_cv_. Protects remaining members.
  SpinLock lock_;

  // signaled when rpc_in_flight_ is set to false.
  condition_variable_any rpc_done_cv_;

  // true if there is a TransmitData() rpc in flight.
  bool rpc_in_flight_ = false;

  // True if we received DATASTREAM_RECVR_ALREADY_GONE.
  bool recvr_gone_ = false;

  // Status of most recently finished TransmitData rpc
  Status last_rpc_status_;
};

Status DataStreamSender::Channel::Init() {
  // TODO: figure out how to size batch_
  int capacity = max(1, buffer_size_ / max(row_desc_->GetRowSize(), 1));
  batch_.reset(new RowBatch(row_desc_, capacity, parent_->mem_tracker()));
  self_ = shared_from_this();
  return Status::OK();
}

Status DataStreamSender::Channel::SendBatch(RowBatch* batch) {
  const shared_ptr<OutboundProtoRowBatch>& proto_batch = proto_batches_[proto_batch_idx_];
  RETURN_IF_ERROR(parent_->SerializeBatch(batch, proto_batch.get()));
  proto_batch_idx_ = (proto_batch_idx_ + 1) % 2;
  return SendSerializedBatch(proto_batch);
}

Status DataStreamSender::Channel::SendSerializedBatch(
    const shared_ptr<OutboundProtoRowBatch>& batch) {
  DCHECK(batch != NULL);

  VLOG_ROW << "Channel::SendBatch() instance_id=" << fragment_instance_id_
           << " dest_node=" << dest_node_id_ << " #rows=" << batch->header.num_rows();
  // return if the previous batch saw an error
  RETURN_IF_ERROR(WaitForClearChannel());
  {
    lock_guard<SpinLock> l(lock_);
    // Return without signalling an error if the upstream receiver has completed, or if
    // this instance is cancelled or closed..
    if (recvr_gone_ || parent_->closed_ || parent_->state_->is_cancelled()) {
      return Status::OK();
    }
    rpc_in_flight_ = true;
  }

  // Completion callback for the TransmitData() RPC which is guaranteed to be called
  // exactly once. The DSS may be cancelled while waiting for this callback, so we need to
  // check that the parent channel still exists by passing in a weak ptr that might
  // expire.
  auto rpc_complete_callback = [self_ptr = weak_ptr<DataStreamSender::Channel>(self_),
      instance_id = fragment_instance_id_, proto_batch = batch]
      (const Status& status, TransmitDataRequestPb* request,
      TransmitDataResponsePb* response, RpcController* controller) {

    // Ensure that request and response get deleted when this callback returns.
    auto request_container = unique_ptr<TransmitDataRequestPb>(request);
    auto response_container = unique_ptr<TransmitDataResponsePb>(response);

    // Check if this channel still exists.
    auto channel = self_ptr.lock();
    if (channel == nullptr) return;
    {
      lock_guard<SpinLock> l(channel->lock_);
      Status rpc_status = status.ok() ? FromKuduStatus(controller->status()) : status;

      int32_t status_code = response->status().status_code();
      channel->recvr_gone_ = status_code == TErrorCode::DATASTREAM_RECVR_ALREADY_GONE;

      if (!rpc_status.ok()) {
        channel->last_rpc_status_ = rpc_status;
      } else if (!channel->recvr_gone_) {
        if (status_code != TErrorCode::OK) {
          // Don't bubble up the 'receiver gone' status, because it's not an error.
          channel->last_rpc_status_ = Status(response->status());
        } else {
          int size = proto_batch->GetSize();
          channel->num_data_bytes_sent_.Add(size);
          VLOG_ROW << "incremented #data_bytes_sent="
                   << channel->num_data_bytes_sent_.Load();
        }
      }
      channel->rpc_in_flight_ = false;
    }
    channel->rpc_done_cv_.notify_one();
  };

  unique_ptr<TransmitDataRequestPb> request = make_unique<TransmitDataRequestPb>();
  request->mutable_dest_fragment_instance_id()->set_lo(fragment_instance_id_.lo);
  request->mutable_dest_fragment_instance_id()->set_hi(fragment_instance_id_.hi);
  request->set_dest_node_id(dest_node_id_);
  request->set_sender_id(parent_->sender_id_);

  unique_ptr<TransmitDataResponsePb> response = make_unique<TransmitDataResponsePb>();
  auto rpc =
      Rpc<DataStreamServiceProxy>::Make(address_, ExecEnv::GetInstance()->rpc_mgr());

  int idx;
  if (batch->header.compression_type() == THdfsCompression::LZ4) {
    rpc.AddSidecar(batch->compressed_tuple_data, &idx);
  } else {
    rpc.AddSidecar(batch->tuple_data, &idx);
  }
  batch->header.set_tuple_data_sidecar_idx(idx);

  rpc.AddSidecar(batch->tuple_offsets, &idx);
  batch->header.set_tuple_offsets_sidecar_idx(idx);
  *request->mutable_row_batch_header() = batch->header;

  // Set the number of attempts very high to try to outlast any situations where the
  // receiver is too busy.
  rpc.SetMaxAttempts(numeric_limits<int32_t>::max()) // Retry until failure or success.
     .SetRetryInterval(10)
     .SetTimeout(MonoDelta::FromMilliseconds(numeric_limits<int32_t>::max()))
     .ExecuteAsync(&DataStreamServiceProxy::TransmitDataAsync, request.release(),
         response.release(), rpc_complete_callback);
  return Status::OK();
}

Status DataStreamSender::Channel::WaitForClearChannel() {
  // Wait until the channel is clear, or the parent sender is closed or cancelled.
  auto timeout = system_clock::now() + milliseconds(50);
  SCOPED_TIMER(parent_->state_->total_network_send_timer());
  auto cond = [this]() -> bool {
    return !rpc_in_flight_ || parent_->closed_ || parent_->state_->is_cancelled();
  };

  unique_lock<SpinLock> l(lock_);
  while (!rpc_done_cv_.wait_until(l, timeout, cond)) {
    timeout = system_clock::now() + milliseconds(50);
  }
  if (!last_rpc_status_.ok()) {
    LOG(ERROR) << "channel send status: " << last_rpc_status_.GetDetail();
  }
  return last_rpc_status_;
}

Status DataStreamSender::Channel::AddRow(TupleRow* row) {
  if (batch_->AtCapacity()) {
    // batch_ is full, let's send it.
    RETURN_IF_ERROR(SendBatch(batch_.get()));
    batch_->Reset();
  }
  TupleRow* dest = batch_->GetRow(batch_->AddRow());
  const vector<TupleDescriptor*>& descs = row_desc_->tuple_descriptors();
  for (int i = 0; i < descs.size(); ++i) {
    if (UNLIKELY(row->GetTuple(i) == NULL)) {
      dest->SetTuple(i, NULL);
    } else {
      dest->SetTuple(i, row->GetTuple(i)->DeepCopy(*descs[i], batch_->tuple_data_pool()));
    }
  }
  batch_->CommitLastRow();
  return Status::OK();
}

Status DataStreamSender::Channel::FlushAndSendEos() {
  VLOG_RPC << "Channel::FlushAndSendEos() instance_id=" << fragment_instance_id_
           << " dest_node=" << dest_node_id_
           << " #rows= " << batch_->num_rows();

  // We can return an error here and not go on to send the EOS RPC because the error that
  // we returned will be sent to the coordinator who will then cancel all the remote
  // fragments including the one that this sender is sending to.
  if (batch_->num_rows() > 0) RETURN_IF_ERROR(SendBatch(batch_.get()));

  RETURN_IF_ERROR(WaitForClearChannel());
  EndDataStreamRequestPb request;
  request.mutable_dest_fragment_instance_id()->set_lo(fragment_instance_id_.lo);
  request.mutable_dest_fragment_instance_id()->set_hi(fragment_instance_id_.hi);
  request.set_dest_node_id(dest_node_id_);
  request.set_sender_id(parent_->sender_id_);

  EndDataStreamResponsePb response;
  RETURN_IF_ERROR(
      Rpc<DataStreamServiceProxy>::Make(address_, ExecEnv::GetInstance()->rpc_mgr())
          .SetTimeout(MonoDelta::FromMilliseconds(FLAGS_rpc_end_datastream_timeout_ms))
          .Execute(&DataStreamServiceProxy::EndDataStream, request, &response));

  return Status::OK();
}

void DataStreamSender::Channel::ReleaseResources() {
  batch_.reset();
  self_.reset();
  proto_batches_.clear();
}

DataStreamSender::DataStreamSender(int sender_id,
    const RowDescriptor* row_desc, const TDataStreamSink& sink,
    const vector<TPlanFragmentDestination>& destinations, int per_channel_buffer_size)
  : DataSink(row_desc),
    sender_id_(sender_id),
    partition_type_(sink.output_partition.type),
    dest_node_id_(sink.dest_node_id) {
  DCHECK_GT(destinations.size(), 0);
  DCHECK(sink.output_partition.type == TPartitionType::UNPARTITIONED
      || sink.output_partition.type == TPartitionType::HASH_PARTITIONED
      || sink.output_partition.type == TPartitionType::RANDOM
      || sink.output_partition.type == TPartitionType::KUDU);
  for (const auto& dest: destinations) {
    channels_.push_back(make_shared<Channel>(this, row_desc, dest.data_svc,
        dest.fragment_instance_id, sink.dest_node_id, per_channel_buffer_size));
  }

  if (partition_type_ == TPartitionType::UNPARTITIONED
      || partition_type_ == TPartitionType::RANDOM) {
    // Randomize the order we open/transmit to channels to avoid thundering herd problems.
    srand(reinterpret_cast<uint64_t>(this));
    random_shuffle(channels_.begin(), channels_.end());
    // Hard-code two batches (one in flight, one as serialization target).
    for (int i = 0; i < 2; ++i) {
      proto_batches_.push_back(make_shared<OutboundProtoRowBatch>());
    }
  }
}

string DataStreamSender::GetName() {
  return Substitute("DataStreamSender (dst_id=$0)", dest_node_id_);
}

Status DataStreamSender::Init(const vector<TExpr>& thrift_output_exprs,
    const TDataSink& tsink, RuntimeState* state) {
  DCHECK(tsink.__isset.stream_sink);
  if (partition_type_ == TPartitionType::HASH_PARTITIONED ||
      partition_type_ == TPartitionType::KUDU) {
    RETURN_IF_ERROR(ScalarExpr::Create(tsink.stream_sink.output_partition.partition_exprs,
        *row_desc_, state, &partition_exprs_));
  }
  return Status::OK();
}

Status DataStreamSender::Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) {
  RETURN_IF_ERROR(DataSink::Prepare(state, parent_mem_tracker));
  state_ = state;
  SCOPED_TIMER(profile_->total_time_counter());
  RETURN_IF_ERROR(ScalarExprEvaluator::Create(partition_exprs_, state,
      state->obj_pool(), expr_mem_pool(), &partition_expr_evals_));
  bytes_sent_counter_ = ADD_COUNTER(profile(), "BytesSent", TUnit::BYTES);
  uncompressed_bytes_counter_ =
      ADD_COUNTER(profile(), "UncompressedRowBatchSize", TUnit::BYTES);
  serialize_batch_timer_ = ADD_TIMER(profile(), "SerializeBatchTime");

  overall_throughput_ =
      profile()->AddDerivedCounter("OverallThroughput", TUnit::BYTES_PER_SECOND,
          [this, total=profile()->total_time_counter()] () {
            return RuntimeProfile::UnitsPerSecond(
                this->bytes_sent_counter_, profile()->total_time_counter());
          });

  total_sent_rows_counter_= ADD_COUNTER(profile(), "RowsReturned", TUnit::UNIT);
  for (const auto& channel : channels_) RETURN_IF_ERROR(channel->Init());
  return Status::OK();
}

Status DataStreamSender::Open(RuntimeState* state) {
  return ScalarExprEvaluator::Open(partition_expr_evals_, state);
}

Status DataStreamSender::Send(RuntimeState* state, RowBatch* batch) {
  DCHECK(!closed_);
  DCHECK(!flushed_);

  // TODO: Handle larger batches than 1024 rows, perhaps by chaining together several row
  // batches in sidecars at once?
  if (batch->num_rows() == 0) return Status::OK();
  if (channels_.size() == 1) {
    RETURN_IF_ERROR(channels_[0]->SendBatch(batch));
  } else if (partition_type_ == TPartitionType::UNPARTITIONED) {
    shared_ptr<OutboundProtoRowBatch> proto_batch = proto_batches_[next_batch_idx_];
    next_batch_idx_ = (next_batch_idx_ + 1) % proto_batches_.size();

    RETURN_IF_ERROR(SerializeBatch(batch, proto_batch.get(), channels_.size()));
    // SendBatch() will block if there are still in-flight rpcs (and those will
    // reference the previously written thrift batch).
    // TODO: Fix this so that slow receivers don't penalize the average case.
    for (auto& channel : channels_) {
      RETURN_IF_ERROR(channel->SendSerializedBatch(proto_batch));
    }
  } else if (partition_type_ == TPartitionType::RANDOM) {
    // Round-robin batches among channels. Wait for the current channel to finish its
    // rpc before overwriting its batch.
    Channel* current_channel = channels_[current_channel_idx_].get();
    RETURN_IF_ERROR(current_channel->SendBatch(batch));
    current_channel_idx_ = (current_channel_idx_ + 1) % channels_.size();
  } else if (partition_type_ == TPartitionType::KUDU) {
    DCHECK_EQ(partition_expr_evals_.size(), 1);
    int num_channels = channels_.size();
    for (int i = 0; i < batch->num_rows(); ++i) {
      TupleRow* row = batch->GetRow(i);
      int32_t partition =
          *reinterpret_cast<int32_t*>(partition_expr_evals_[0]->GetValue(row));
      if (partition < 0) {
        // This row doesn't correspond to a partition, e.g. it's outside the given ranges.
        partition = next_unknown_partition_;
        ++next_unknown_partition_;
      }
      channels_[partition % num_channels]->AddRow(row);
    }
  } else {
    DCHECK(partition_type_ == TPartitionType::HASH_PARTITIONED);
    // hash-partition batch's rows across channels
    // TODO: encapsulate this in an Expr as we've done for Kudu above and remove this case
    // once we have codegen here.
    int num_channels = channels_.size();
    for (int i = 0; i < batch->num_rows(); ++i) {
      TupleRow* row = batch->GetRow(i);
      uint32_t hash_val = HashUtil::FNV_SEED;
      for (int i = 0; i < partition_exprs_.size(); ++i) {
        ScalarExprEvaluator* eval = partition_expr_evals_[i];
        void* partition_val = eval->GetValue(row);
        // We can't use the crc hash function here because it does not result
        // in uncorrelated hashes with different seeds.  Instead we must use
        // fnv hash.
        // TODO: fix crc hash/GetHashValue()
        DCHECK(&partition_expr_evals_[i]->root() == partition_exprs_[i]);
        hash_val = RawValue::GetHashValueFnv(
            partition_val, partition_exprs_[i]->type(), hash_val);
      }
      ScalarExprEvaluator::FreeLocalAllocations(partition_expr_evals_);
      RETURN_IF_ERROR(channels_[hash_val % num_channels]->AddRow(row));
    }
  }
  COUNTER_ADD(total_sent_rows_counter_, batch->num_rows());
  RETURN_IF_ERROR(state->CheckQueryState());
  return Status::OK();
}

Status DataStreamSender::FlushFinal(RuntimeState* state) {
  DCHECK(!flushed_);
  DCHECK(!closed_);
  flushed_ = true;
  // If we hit an error here, we can return without closing the remaining channels as
  // the error is propagated back to the coordinator, which in turn cancels the query,
  // which will cause the remaining open channels to be closed.
  for (auto& channel: channels_) RETURN_IF_ERROR(channel->FlushAndSendEos());

  return Status::OK();
}

void DataStreamSender::Close(RuntimeState* state) {
  if (closed_) return;
  for (auto& channel: channels_) channel->ReleaseResources();
  ScalarExprEvaluator::Close(partition_expr_evals_, state);
  ScalarExpr::Close(partition_exprs_);
  DataSink::Close(state);
  closed_ = true;
}

Status DataStreamSender::SerializeBatch(
    RowBatch* src, OutboundProtoRowBatch* dest, int num_receivers) {
  VLOG_ROW << "serializing " << src->num_rows() << " rows";
  {
    SCOPED_TIMER(profile_->total_time_counter());
    SCOPED_TIMER(serialize_batch_timer_);
    RETURN_IF_ERROR(src->Serialize(dest));
    int bytes = dest->GetSize();
    int uncompressed_bytes =
        bytes - dest->tuple_data->length() + dest->header.uncompressed_size();
    // The size output_batch would be if we didn't compress tuple_data (will be equal to
    // actual batch size if tuple_data isn't compressed)

    COUNTER_ADD(bytes_sent_counter_, bytes * num_receivers);
    COUNTER_ADD(uncompressed_bytes_counter_, uncompressed_bytes * num_receivers);
  }
  return Status::OK();
}

int64_t DataStreamSender::GetNumDataBytesSent() const {
  // TODO: do we need synchronization here or are reads & writes to 8-byte ints
  // atomic?
  int64_t result = 0;
  for (auto& channel: channels_) result += channel->num_data_bytes_sent();
  return result;
}

}
