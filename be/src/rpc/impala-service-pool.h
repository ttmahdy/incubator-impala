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

#ifndef IMPALA_RPC_IMPALA_SERVICE_POOL_H
#define IMPALA_RPC_IMPALA_SERVICE_POOL_H

#include "gutil/gscoped_ptr.h"

#include "kudu/util/metrics.h"
#include "kudu/rpc/rpc_service.h"
#include "kudu/rpc/service_if.h"
#include "kudu/rpc/service_queue.h"
#include "util/histogram-metric.h"
#include "util/spinlock.h"
#include "util/thread.h"

#include <rapidjson/document.h>

#include <memory>

namespace impala {

/// Heavily based on kudu::rpc::ServicePool. Implements the RpcService interface, using
/// Impala threads to handle RPC requests so that they are exposed to instrumentation.
/// Also records per-request metrics like handling time, queuing time, and payload size.
class ImpalaServicePool : public kudu::rpc::RpcService {
 public:
  ImpalaServicePool(std::unique_ptr<kudu::rpc::ServiceIf> service,
      const scoped_refptr<kudu::MetricEntity>& metric_entity,
      size_t svc_queue_depth);
  virtual ~ImpalaServicePool() { }

  /// Initialises the pool with 'num_threads' threads.
  Status Init(int32_t num_threads);

  /// Shuts down the thread pool and waits for the threads to terminate.
  void Shutdown();

  // RpcService required methods.
  kudu::rpc::RpcMethodInfo* LookupMethod(const kudu::rpc::RemoteMethod& method) override;
  kudu::Status QueueInboundCall(gscoped_ptr<kudu::rpc::InboundCall> call) override;

  const std::string service_name() const;

  /// Populates a JSON document with diagnostic information.
  void ToJson(rapidjson::Value* value, rapidjson::Document* document);

 private:
  void RunThread();
  void RejectTooBusy(kudu::rpc::InboundCall* c);

  std::unique_ptr<kudu::rpc::ServiceIf> service_;
  std::vector<std::unique_ptr<Thread>> threads_;

  /// Distribution of time spent in the RPC callback (which runs on the service threads)
  /// across all methods for this service pool.
  std::unique_ptr<HistogramMetric> request_handle_time_;

  /// Distribution of time spent for all incoming requests in the service queue, prior to
  /// handling.
  std::unique_ptr<HistogramMetric> incoming_queue_time_;

  struct MethodInfo {
    std::unique_ptr<HistogramMetric> handling_time;
    std::unique_ptr<HistogramMetric> payload_size;
    AtomicInt32 num_in_handlers;
  };

  /// Protects method_infos_
  SpinLock method_info_lock_;

  /// Method
  std::map<std::string, MethodInfo> method_infos_;

  AtomicInt32 rpcs_timed_out_in_queue_;
  AtomicInt32 rpcs_queue_overflow_;

  /// Queue of requests. Note, despite its name, that the request order is FIFO - LIFO
  /// refers to the order in which threads are selected to run the service requests.
  kudu::rpc::LifoServiceQueue service_queue_;

  /// Dummy histogram needed to call InboundCall::RecordHandlingStarted(). Unused
  /// otherwise.
  scoped_refptr<kudu::Histogram> unused_histogram_;

  /// Protects closing_.
  boost::mutex shutdown_lock_;
  bool closing_ = false;
};

}

#endif
