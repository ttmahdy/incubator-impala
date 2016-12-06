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

#ifndef IMPALA_RPC_RPC_MGR_H
#define IMPALA_RPC_RPC_MGR_H

#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/service_pool.h"

#include "rpc/impala-service-pool.h"

#include <rapidjson/document.h>

#include "common/status.h"

namespace kudu {
namespace rpc {
class ServiceIf;
class ResultTracker;
}
}

namespace impala {

class Webserver;

/// Central manager for all RPC services and proxies.
///
/// SERVICES
/// --------
///
/// An RpcMgr manages 0 or more services: RPC interfaces that are a collection of remotely
/// accessible methods. A new service is registered by calling RegisterService(). All
/// services are served on the same port; the underlying RPC layer takes care of
/// de-multiplexing RPC calls to their respective endpoints.
///
/// Services are made available to remote clients when RpcMgr::StartServices() is called;
/// before this method no service method will be called.
///
/// Services may only be registered and started after RpcMgr::Init() is called.
///
/// PROXIES
/// -------
///
/// A proxy is a client-side interface to a remote service. Remote methods exported by
/// that service may be called through a proxy as though they were local methods.
///
/// A proxy can be obtained by calling GetProxy(). Proxies are cheap to create (relative
/// to the cost of an remote method call) and need not be cached. Proxies implement local
/// methods which call remote service methods, e.g. proxy->Foo(request, &response) will
/// call the Foo() service method on the service that 'proxy' points to.
///
/// Proxies may only be created after RpcMgr::Init() is called.
///
/// LIFECYCLE
/// ---------
///
/// Before any proxy or service interactions, RpcMgr::Init() must be called to start the
/// reactor threads that service network events. Services must be registered with
/// RpcMgr::RegisterService() before RpcMgr::StartServices() is called.
///
/// When shutting down, clients must call UnregisterServices() to ensure that all services
/// are cleanly terminated. Proxies may still be obtained and used until the RpcMgr is
/// destroyed.
///
/// KRPC INTERNALS
/// --------------
///
/// Each service and proxy interacts with the network via a shared pool of 'reactor'
/// threads which respond to incoming and outgoing RPC events. Incoming events are passed
/// immediately to one of two thread pools: new connections are handled by an 'acceptor'
/// pool, and RPC request events are handled by a per-service 'service' pool. The size of
/// these pools may be configured by RegisterService().
///
/// If the rate of incoming RPC requests exceeds the rate at which those requests are
/// processed, some requests will be placed in a FIFO fixed-size queue. If the queue
/// becomes full, the RPC will fail at the caller. The size of the queue is configurable
/// by RegisterService().
///
/// Inbound connection set-up is handled by a small fixed-size pool of 'acceptor'
/// threads. The number of threads that accept new TCP connection requests to the service
/// port is configurable by StartServices().
class RpcMgr {
 public:
  /// Initialises the reactor threads, and prepares for sending outbound RPC requests.
  Status Init(int32_t num_reactor_threads);

  bool is_inited() const { return messenger_.get() != nullptr; }

  /// Start the acceptor threads, making RPC services available. Before this method is
  /// called, remote clients will get a 'connection refused' error when trying to invoke
  /// an RPC on this machine.
  Status StartServices(int32_t port, int32_t num_acceptor_threads);

  /// Register a new service.
  ///
  /// 'num_service_threads' is the number of threads that should be started to execute RPC
  /// handlers for the new service.
  ///
  /// 'service_queue_depth' is the maximum number of requests that may be queued for this
  /// service before clients being to see rejection errors.
  ///
  /// 'service_ptr' contains an interface implementation that will handle RPCs.
  ///
  /// It is an error to call this after StartServices() has been called.
  Status RegisterService(int32_t num_service_threads, int32_t service_queue_depth,
      std::unique_ptr<kudu::rpc::ServiceIf> service_ptr);

  /// Creates a new proxy for a remote service of type P at location 'address', and places
  /// it in 'proxy'. Returns an error if 'address' cannot be resolved to an IP address.
  ///
  /// 'P' must descend from kudu::rpc::ServiceIf.
  template <typename P>
  Status GetProxy(const TNetworkAddress& address, std::unique_ptr<P>* proxy);

  /// Unregisters all previously registered services. The RPC layer continues to run.
  void UnregisterServices();

  const scoped_refptr<kudu::rpc::ResultTracker> result_tracker() const {
    return tracker_;
  }

  scoped_refptr<kudu::MetricEntity> metric_entity() const {
    return messenger_->metric_entity();
  }

  kudu::rpc::Messenger* messenger() { return messenger_.get(); }

  ~RpcMgr() {
    DCHECK_EQ(service_pools_.size(), 0)
        << "Must call UnregisterServices() before destroying RpcMgr";
  }

  void ToJson(rapidjson::Document* document);

 private:
  /// One pool per registered service.
  std::vector<scoped_refptr<ImpalaServicePool>> service_pools_;

  /// Required Kudu boilerplate.
  /// TODO(KRPC): Integrate with Impala MetricGroup.
  kudu::MetricRegistry registry_;

  /// Shared with all service objects (see RegisterService()).
  const scoped_refptr<kudu::rpc::ResultTracker> tracker_;

  /// Container for reactor threads which run event loops for RPC services, plus acceptor
  /// threads which manage connection setup.
  std::shared_ptr<kudu::rpc::Messenger> messenger_;

  /// True after StartServices() completes.
  bool services_started_ = false;
};
}

#endif
