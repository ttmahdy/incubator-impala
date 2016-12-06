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

#include "rpc/impala-service-pool.h"

#include "kudu/util/blocking_queue.h"
#include "kudu/util/trace.h"
#include "kudu/rpc/inbound_call.h"
#include "gutil/strings/substitute.h"

#include "common/names.h"

using namespace rapidjson;

using kudu::MonoTime;
using kudu::Histogram;
using kudu::MetricEntity;
using kudu::rpc::ErrorStatusPB;
using kudu::rpc::InboundCall;
using kudu::rpc::RemoteMethod;
using kudu::rpc::RpcMethodInfo;
using kudu::rpc::ServiceIf;
using kudu::rpc::QueueStatus;

METRIC_DEFINE_histogram(server, impala_unused,
    "RPC Queue Time",
    kudu::MetricUnit::kMicroseconds,
    "Number of microseconds incoming RPC requests spend in the worker queue",
    60000000LU, 3);

namespace impala {

ImpalaServicePool::ImpalaServicePool(
    unique_ptr<ServiceIf> service_if,
    const scoped_refptr<MetricEntity>& metric_entity,
    size_t service_queue_depth)
    : service_(move(service_if)), service_queue_(service_queue_depth),
      unused_histogram_(METRIC_impala_unused.Instantiate(metric_entity)) {

  string name = Substitute("$0-queue-time", service_->service_name());
  incoming_queue_time_.reset(new HistogramMetric(
          MakeTMetricDef(name, TMetricKind::HISTOGRAM, TUnit::TIME_MS),
          30000, 3));
  request_handle_time_.reset(new HistogramMetric(
          MakeTMetricDef(name, TMetricKind::HISTOGRAM, TUnit::TIME_MS),
          30000, 3));
}

Status ImpalaServicePool::Init(int32_t num_threads) {
  for (int i = 0; i < num_threads; ++i) {
    unique_ptr<Thread> thread = make_unique<Thread>(Substitute("$0-service-pool", service_name()),
        Substitute("Worker $0 (of $1)", i + 1, num_threads),
        [this]() {
          RunThread();
        });
    threads_.push_back(move(thread));
  }
  return Status::OK();
}

void ImpalaServicePool::Shutdown() {
  service_queue_.Shutdown();

  lock_guard<mutex> lock(shutdown_lock_);
  if (closing_) return;
  closing_ = true;
  for (auto& thread : threads_) {
    thread->Join();
  }

  // Now we must drain the service queue.
  kudu::Status status = kudu::Status::ServiceUnavailable("Service is shutting down");
  unique_ptr<InboundCall> incoming;
  while (service_queue_.BlockingGet(&incoming)) {
    incoming.release()->RespondFailure(ErrorStatusPB::FATAL_SERVER_SHUTTING_DOWN, status);
  }

  service_->Shutdown();
}

void ImpalaServicePool::RunThread() {
  while (true) {
    unique_ptr<InboundCall> incoming;
    if (!service_queue_.BlockingGet(&incoming)) {
      VLOG(1) << "ServicePool: messenger shutting down.";
      return;
    }
    incoming->RecordHandlingStarted(unused_histogram_);
    MonoTime now = MonoTime::Now();
    int total_time = (now - incoming->GetTimeReceived()).ToMilliseconds();
    incoming_queue_time_->Update(total_time);
    ADOPT_TRACE(incoming->trace());

    if (PREDICT_FALSE(incoming->ClientTimedOut())) {
      TRACE_TO(incoming->trace(), "Skipping call since client already timed out");
      rpcs_timed_out_in_queue_.Add(1);

      // Respond as a failure, even though the client will probably ignore
      // the response anyway.
      incoming->RespondFailure(
        ErrorStatusPB::ERROR_SERVER_TOO_BUSY,
        kudu::Status::TimedOut("Call waited in the queue past client deadline"));

      // Must release since RespondFailure above ends up taking ownership
      // of the object.
      ignore_result(incoming.release());
      continue;
    }

    TRACE_TO(incoming->trace(), "Handling call");

    // Release the InboundCall pointer -- when the call is responded to,
    // it will get deleted at that point.
    string method_name = incoming->remote_method().method_name();
    int32_t size = incoming->GetTransferSize();
    {
      lock_guard<SpinLock> l(method_info_lock_);
      auto* method = &method_infos_[method_name];
      method->num_in_handlers.Add(1);
    }
    service_->Handle(incoming.release());
    total_time = (MonoTime::Now() - now).ToMilliseconds();
    request_handle_time_->Update(total_time);

    {
      lock_guard<SpinLock> l(method_info_lock_);
      auto* method = &method_infos_[method_name];
      method->num_in_handlers.Add(-1);
      if (method->handling_time.get() == nullptr) {
        string name = Substitute("$0-handling-time", method_name);
        method->handling_time.reset(new HistogramMetric(
                MakeTMetricDef(name, TMetricKind::HISTOGRAM, TUnit::TIME_MS),
                300000, 3));
        name = Substitute("$0-payload-size", method_name);
        method->payload_size.reset(new HistogramMetric(
                MakeTMetricDef(name, TMetricKind::HISTOGRAM, TUnit::BYTES),
                1024 * 1024 * 1024, 3));
      }
      method->handling_time->Update(total_time);
      method->payload_size->Update(size);
    }
  }
}

const string ImpalaServicePool::service_name() const {
  return service_->service_name();
}

void ImpalaServicePool::RejectTooBusy(InboundCall* c) {
  string err_msg =
      Substitute("$0 request on $1 from $2 dropped due to backpressure. "
          "The service queue is full; it has $3 items.",
          c->remote_method().method_name(),
          service_->service_name(),
          c->remote_address().ToString(),
          service_queue_.max_size());
  rpcs_queue_overflow_.Add(1);
  c->RespondFailure(ErrorStatusPB::ERROR_SERVER_TOO_BUSY,
      kudu::Status::ServiceUnavailable(err_msg));
  VLOG(1) << err_msg << " Contents of service queue:\n"
             << service_queue_.ToString();
}

RpcMethodInfo* ImpalaServicePool::LookupMethod(const RemoteMethod& method) {
  return service_->LookupMethod(method);
}


kudu::Status ImpalaServicePool::QueueInboundCall(gscoped_ptr<InboundCall> call) {
  InboundCall* c = call.release();

  vector<uint32_t> unsupported_features;
  for (uint32_t feature : c->GetRequiredFeatures()) {
    if (!service_->SupportsFeature(feature)) {
      unsupported_features.push_back(feature);
    }
  }

  using KStatus = kudu::Status;
  if (!unsupported_features.empty()) {
    c->RespondUnsupportedFeature(unsupported_features);
    return KStatus::NotSupported("call requires unsupported application feature flags");
  }

  TRACE_TO(c->trace(), "Inserting onto call queue");

  // Queue message on service queue
  boost::optional<InboundCall*> evicted;
  auto queue_status = service_queue_.Put(c, &evicted);
  if (queue_status == QueueStatus::QUEUE_FULL) {
    RejectTooBusy(c);
    return KStatus::OK();
  }

  if (PREDICT_FALSE(evicted != boost::none)) {
    RejectTooBusy(*evicted);
  }

  if (PREDICT_TRUE(queue_status == QueueStatus::QUEUE_SUCCESS)) {
    // NB: do not do anything with 'c' after it is successfully queued --
    // a service thread may have already dequeued it, processed it, and
    // responded by this point, in which case the pointer would be invalid.
    return KStatus::OK();
  }

  KStatus status = KStatus::OK();
  if (queue_status == QueueStatus::QUEUE_SHUTDOWN) {
    status = KStatus::ServiceUnavailable("Service is shutting down");
    c->RespondFailure(ErrorStatusPB::FATAL_SERVER_SHUTTING_DOWN, status);
  } else {
    status = KStatus::RuntimeError(
        Substitute("Unknown error from BlockingQueue: $0", queue_status));
    c->RespondFailure(ErrorStatusPB::FATAL_UNKNOWN, status);
  }
  return status;
}

void ImpalaServicePool::ToJson(Value* value, Document* document) {
  int queue_size = service_queue_.estimated_queue_length();
  int idle_threads = service_queue_.estimated_idle_worker_count();
  value->AddMember("queue_size", queue_size, document->GetAllocator());
  value->AddMember("idle_thread", idle_threads, document->GetAllocator());

  Value name(service_name().c_str(), document->GetAllocator());
  value->AddMember("name", name, document->GetAllocator());
  value->AddMember("max_queue_size", service_queue_.max_size(), document->GetAllocator());
  value->AddMember("rpcs_timed_out_in_queue", rpcs_timed_out_in_queue_.Load(),
      document->GetAllocator());
  value->AddMember("rpcs_queue_overflow", rpcs_queue_overflow_.Load(),
      document->GetAllocator());

  Value queuing_histogram(kObjectType);
  incoming_queue_time_->ToJson(document, &queuing_histogram);
  value->AddMember(
      "rpc_queue_time_histogram", queuing_histogram, document->GetAllocator());

  Value handling_histogram(kObjectType);
  request_handle_time_->ToJson(document, &handling_histogram);
  value->AddMember(
      "rpc_handle_time_histogram", handling_histogram, document->GetAllocator());

  Value method_histograms(kArrayType);
  {
    lock_guard<SpinLock> l(method_info_lock_);
    for (const auto& entry: method_infos_) {
      Value method(kObjectType);
      Value name(entry.first.c_str(), document->GetAllocator());
      method.AddMember("name", name, document->GetAllocator());
      method.AddMember("num_in_handlers", entry.second.num_in_handlers.Load(),
          document->GetAllocator());
      Value time_histogram(kObjectType);
      entry.second.handling_time->ToJson(document, &time_histogram);
      method.AddMember("handling_histogram", time_histogram, document->GetAllocator());

      Value size_histogram(kObjectType);
      entry.second.payload_size->ToJson(document, &size_histogram);
      method.AddMember(
          "payload_size_histogram", size_histogram, document->GetAllocator());
      method_histograms.PushBack(method, document->GetAllocator());
    }
  }
  value->AddMember("methods", method_histograms, document->GetAllocator());
}

}
