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


#ifndef IMPALA_RUNTIME_DATA_STREAM_MGR_H
#define IMPALA_RUNTIME_DATA_STREAM_MGR_H

#include <set>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

#include "common/status.h"
#include "runtime/descriptors.h"  // for PlanNodeId
#include "runtime/row-batch.h"
#include "util/metrics.h"
#include "util/promise.h"
#include "util/thread-pool.h"
#include "gen-cpp/Types_types.h"  // for TUniqueId

namespace kudu { namespace rpc { class RpcContext; } }

namespace impala {

class DescriptorTbl;
class DataStreamRecvr;
class RuntimeState;
class TransmitDataRequestPb;
class TransmitDataResponsePb;

/// TRANSMIT DATA PROTOCOL
/// ----------------------
///
/// Impala daemons send tuple data between themselves using a transmission protocol that
/// is managed by DataStreamMgr and related classes. Batches of tuples are sent using the
/// TransmitData() RPC; since more data is usually transmitted than fits into a single
/// RPC, we refer to the ongoing transmission from a client to a server as a 'stream', and
/// the logical connection between them as a 'channel'. Clients and servers are referred
/// to as 'senders' and 'receivers' and are implemented by DataStreamSender and
/// DataStreamRecvr respectively. DataStreamMgr is a singleton class that lives for as
/// long as the Impala process, and manages all streams for all queries. DataStreamRecvr
/// and DataStreamSender have lifetimes tied to their containing fragment instances.
///
/// The protocol proceeds in three phases.
///
/// Phase 1: Channel establishment
/// ------------------------------
///
/// In the first phase the sender initiates a channel with the receiver by sending its
/// first batch. Since the sender may start sending before the receiver is ready, the data
/// stream manager waits until the receiver has finished initialization and then passes
/// the sender's request to the receiver. If the receiver does not appear within a
/// configurable timeout, the data stream manager notifies the sender directly by
/// returning DATASTREAM_SENDER_TIMEOUT.
///
/// The sender does not distinguish this phase from the steady-state data transmission
/// phase, so may time-out etc. as described below.
///
/// Phase 2: Data transmission
/// --------------------------
///
/// After the first batch has been received, the sender continues to send batches, one at
/// a time (so only one TransmitData() RPC per sender is pending completion at any one
/// time). The rate of transmission is controlled by the receiver: a sender will only
/// schedule batch transmission when the previous transmission completes
/// successfully. When a batch is received, a receiver will do one of three things:
/// process it immediately, add it to a fixed-size 'batch queue' for later processing, or
/// postpone processing the RPC if the buffer is full. In the first two cases, the sender
/// is notified when the batch has been processed, or added to the batch queue,
/// respectively. The sender will then send its next batch.
///
/// In the third case, the sender is added to a list of pending senders - those who have
/// batches to send, but there's no available space for their tuple data. When space
/// becomes available in the batch queue, the longest-waiting RPC is removed from the
/// pending sender queue and re-processed, with the same three possible outcomes as
/// before.
///
/// Phase 3: End of stream
/// ----------------------
///
/// When the stream is terminated, the client will send an EndDataStream() RPC to the
/// server. During ordinary operation, this RPC will not be sent until after the final
/// TransmitData() RPC has completed and the stream's contents has been delivered. After
/// EndDataStream() is sent, no TransmitData() RPCs will successfully deliver their
/// payload.
///
/// Exceptional conditions: cancellation, timeouts, failure
/// -------------------------------------------------------
///
/// The protocol must deal with the following complications: asynchronous cancellation of
/// either the receiver or sender, timeouts during RPC transmission, and failure of either
/// the receiver or sender.
///
/// 1. Cancellation
///
/// If the receiver is cancelled (or closed for any other reason, like reaching a limit)
/// before the sender has completed the stream it will be torn down immediately. Any
/// incomplete senders may not be aware of this, and will continue to send batches. The
/// data stream manager on the receiver keeps a record of recently completed receivers so
/// that it may intercept the 'late' data transmissions and immediately reject them with
/// an error that signals the sender should terminate.
///
/// In exceptional circumstances, the data stream manager will garbage-collect the closed
/// receiver record before all senders have completed. In that case the sender will
/// time-out and cancel itself. However, it is usual that the coordinator will initiate
/// cancellation in this case and so the sender will be cancelled well before its timeout
/// period expires.
///
/// The sender RPCs are sent asynchronously to the main thread of fragment instance
/// execution. Senders do not block in TransmitData() RPCs, and may be cancelled at any
/// time. If an RPC is in-flight during cancellation, it will quietly drop its result when
/// it returns as the sender object may have been destroyed.
///
/// 2. Timeouts during RPC transmission
///
/// Since RPCs may be arbitrarily delayed in the pending sender queue, the TransmitData()
/// RPC has no RPC-level timeout. Instead, the receiver returns an error to the sender if
/// a timeout occurs during the initial channel establishment phase. Since the
/// TransmitData() RPC is asynchronous from the sender, the sender may continue to check
/// for cancellation while it is waiting for a response from the receiver.
///
/// 3. Node or instance failure
///
/// If the receiver node fails, RPCs will fail fast and the fragment instance will be
/// cancelled.
///
/// If a sender node fails, or the receiver node hangs, the coordinator should detect the
/// failure and cancel all fragments.
///
/// Future improvements
/// -------------------
///
/// * Consider tracking, on the sender, whether a batch has been successfully sent or
///   not. That's enough state to realise that a recvr has failed (rather than not
///   prepared yet), and the data stream mgr can use that to fail an RPC fast, rather than
///   having the closed-stream list. The case where a receiver starts and fails
///   immediately would not be handled - but perhaps it's ok to rely on coordinator
///   cancellation in that case.

/// Context for a TransmitData() RPC. This structure is passed out of the RPC handler
/// itself, and queued by the data stream manager for processing and response.
struct TransmitDataCtx {
  /// Serialized row batch attached to the request. The memory for the row batch is owned
  /// by the 'context' below.
  InboundProtoRowBatch proto_batch;

  /// Must be responded to once this RPC is finished with. Like all RpcContexts, will
  /// delete itself once it has been responded to.
  kudu::rpc::RpcContext* context;

  /// Request data structure, memory owned by 'context'.
  const TransmitDataRequestPb* request;

  /// Response data structure, will be serialized back to client after 'context' is
  /// responded to.
  TransmitDataResponsePb* response;

  /// Time of construction, used to age unresponded-to RPCs out of the queue for a
  /// channel.
  int64_t arrival_time_ms;

  TransmitDataCtx(const InboundProtoRowBatch& batch, kudu::rpc::RpcContext* context,
     const TransmitDataRequestPb* request, TransmitDataResponsePb* response)
      : proto_batch(batch), context(context), request(request), response(response),
        arrival_time_ms(MonotonicMillis()) { }

  TransmitDataCtx() { }
};

/// Singleton class which manages all incoming data streams at a backend node. It
/// provides both producer and consumer functionality for each data stream.
///
/// - RPC service threads use this to add incoming data to streams in response to
///   TransmitData rpcs (AddData()) or to signal end-of-stream conditions (CloseSender()).
/// - Exchange nodes extract data from an incoming stream via a DataStreamRecvr, which is
///   created with CreateRecvr().
//
/// DataStreamMgr also allows asynchronous cancellation of streams via Cancel()
/// which unblocks all DataStreamRecvr::GetBatch() calls that are made on behalf
/// of the cancelled fragment id.
///
/// Exposes three metrics:
///  'senders-blocked-on-recvr-creation' - currently blocked senders.
///  'total-senders-blocked-on-recvr-creation' - total number of blocked senders over
///  time.
///  'total-senders-timedout-waiting-for-recvr-creation' - total number of senders that
///  timed-out while waiting for a receiver.
///
/// TODO: The recv buffers used in DataStreamRecvr should count against
/// per-query memory limits.
class DataStreamMgr {
 public:
  DataStreamMgr(MetricGroup* metrics);

  /// Create a receiver for a specific fragment_instance_id/node_id destination;
  /// If is_merging is true, the receiver maintains a separate queue of incoming row
  /// batches for each sender and merges the sorted streams from each sender into a
  /// single stream.
  /// Ownership of the receiver is shared between this DataStream mgr instance and the
  /// caller.
  std::shared_ptr<DataStreamRecvr> CreateRecvr(RuntimeState* state,
      const RowDescriptor* row_desc, const TUniqueId& fragment_instance_id,
      PlanNodeId dest_node_id, int num_senders, int buffer_size, RuntimeProfile* profile,
      bool is_merging);

  /// Adds a row batch to the recvr identified by fragment_instance_id. The destination
  /// node ID and the sender ID are contained in the RPC request structure. If the recvr
  /// has not yet prepared 'payload' is queued until it arrives, or is timed out. If the
  /// recvr has already been torn-down (within the last STREAM_EXPIRATION_TIME_MS), the
  /// sender will be responded to immediately. Otherwise the sender will time out.
  ///
  /// The RPC encapsulated by 'payload' is guaranteed to be responded to once AddData() is
  /// called, but may not be responded to by the time it returns.
  ///
  /// If the stream would exceed its buffering limit as a result of queuing this batch,
  /// the batch is discarded and the sender is queued for later notification that it
  /// should retry this transmission.
  ///
  /// TODO: enforce per-sender quotas (something like 200% of buffer_size/#senders),
  /// so that a single sender can't flood the buffer and stall everybody else.
  /// Returns OK if successful, error status otherwise.
  void AddData(const TUniqueId& fragment_instance_id,
      std::unique_ptr<TransmitDataCtx>&& payload);

  /// Notifies the recvr associated with the fragment/node id that the specified
  /// sender has closed.
  /// Returns OK if successful, error status otherwise.
  Status CloseSender(const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id,
      int sender_id);

  /// Closes all receivers registered for fragment_instance_id immediately.
  void Cancel(const TUniqueId& fragment_instance_id);

  /// Waits for maintenance thread and sender response thread pool to finish.
  ~DataStreamMgr();

 private:
  friend class DataStreamRecvr;

  /// Set of threads which deserialize buffered row batches, and deliver them to their
  /// receivers. Used only for RPCs which were buffered after their channel's batch queue
  /// was full.
  struct DeserializeWorkItem {
    TUniqueId fragment_instance_id;
    std::unique_ptr<TransmitDataCtx> ctx;
  };
  ThreadPool<DeserializeWorkItem> deserialize_pool_;

  /// Add an RPC context to the deserialization pool.
  void EnqueueRowBatch(DeserializeWorkItem&& payload);

  /// Periodically, notify all senders that have waited for too long for their receiver to
  /// show up.
  boost::scoped_ptr<Thread> maintenance_thread_;

  /// Used to notify maintenance_thread_ that it should exit.
  Promise<bool> shutdown_promise_;

  /// Current number of senders waiting for a receiver to register
  IntGauge* num_senders_waiting_;

  /// Total number of senders that have ever waited for a receiver to register
  IntCounter* total_senders_waited_;

  /// Total number of senders that timed-out waiting for a receiver to register
  IntCounter* num_senders_timedout_;

  /// protects all fields below
  boost::mutex lock_;

  /// map from hash value of fragment instance id/node id pair to stream receivers;
  /// Ownership of the stream revcr is shared between this instance and the caller of
  /// CreateRecvr().
  /// we don't want to create a map<pair<TUniqueId, PlanNodeId>, DataStreamRecvr*>,
  /// because that requires a bunch of copying of ids for lookup
  typedef boost::unordered_multimap<uint32_t, std::shared_ptr<DataStreamRecvr>> RecvrMap;
  RecvrMap receiver_map_;

  /// (Fragment instance id, Plan node id) pair that uniquely identifies a stream.
  typedef std::pair<impala::TUniqueId, PlanNodeId> RecvrId;

  /// Less-than ordering for RecvrIds.
  struct ComparisonOp {
    bool operator()(const RecvrId& a, const RecvrId& b) {
      if (a.first.hi < b.first.hi) {
        return true;
      } else if (a.first.hi > b.first.hi) {
        return false;
      } else if (a.first.lo < b.first.lo) {
        return true;
      } else if (a.first.lo > b.first.lo) {
        return false;
      }
      return a.second < b.second;
    }
  };

  /// Ordered set of receiver IDs so that we can easily find all receivers for a given
  /// fragment (by starting at (fragment instance id, 0) and iterating until the fragment
  /// instance id changes), which is required for cancellation of an entire fragment.
  ///
  /// There is one entry in fragment_recvr_set_ for every entry in receiver_map_.
  typedef std::set<RecvrId, ComparisonOp> FragmentRecvrSet;
  FragmentRecvrSet fragment_recvr_set_;

  /// Return the receiver for given fragment_instance_id/node_id, or an empty shared_ptr
  /// if not found. Must be called with lock_ already held. If the stream was recently
  /// closed, sets *already_unregistered to true to indicate to caller that stream will
  /// not be available in the future. In that case, the returned shared_ptr will be empty.
  std::shared_ptr<DataStreamRecvr> FindRecvr(const TUniqueId& fragment_instance_id,
      PlanNodeId node_id, bool* already_unregistered);

  /// Remove receiver block for fragment_instance_id/node_id from the map.
  Status DeregisterRecvr(const TUniqueId& fragment_instance_id, PlanNodeId node_id);

  inline uint32_t GetHashValue(const TUniqueId& fragment_instance_id, PlanNodeId node_id);

  /// Notifies any sender that has been waiting for its receiver for more than
  /// FLAGS_datastream_sender_timeout_ms.
  ///
  /// Run by maintenance_thread_.
  void Maintenance();

  /// List of waiting senders that need to be processed when a receiver is created.
  /// Access is only thread-safe when lock_ is held.
  struct EarlySendersList {
    /// List of senders that called AddData() before the receiver was set up.
    std::vector<std::unique_ptr<TransmitDataCtx>> waiting_senders;

    /// List of senders that called EndDataStream() before the receiver was set up.
    std::vector<int32_t> closing_senders;

    /// Time of arrival of the first sender. Used to notify senders when they have waited
    /// too long.
    int64_t arrival_time;

    EarlySendersList() : arrival_time(MonotonicMillis()) { }
  };

  /// Map from stream (which identifies a receiver) to a list of senders that should be
  /// processed when that receiver arrives.
  ///
  /// Entries are removed from early_senders_ when either a) a receiver is created or b)
  /// the Maintenance() thread detects that the longest-waiting sender has been waiting
  /// for more than FLAGS_datastream_sender_timeout_ms.
  typedef boost::unordered_map<RecvrId, EarlySendersList> EarlySendersMap;
  EarlySendersMap early_senders_;

  /// Map from the time, in ms, that a stream should be evicted from closed_stream_cache
  /// to its RecvrId. Used to evict old streams from cache efficiently. multimap in case
  /// there are multiple streams with the same eviction time.
  typedef std::multimap<int64_t, RecvrId> ClosedStreamMap;
  ClosedStreamMap closed_stream_expirations_;

  /// Cache of recently closed RecvrIds. Used to allow straggling senders to fail fast by
  /// checking this cache, rather than waiting for the missed-receiver timeout to elapse.
  boost::unordered_set<RecvrId> closed_stream_cache_;
};

}

#endif
