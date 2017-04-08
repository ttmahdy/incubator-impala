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

#ifndef IMPALA_RPC_RPC_BUILDER_H
#define IMPALA_RPC_RPC_BUILDER_H

#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/util/monotime.h"
#include "rpc/common.pb.h"
#include "rpc/rpc-mgr.inline.h"
#include "rpc/thrift-util.h"
#include "util/time.h"

#include "common/status.h"

namespace impala {

/// Helper class to automate much of the boilerplate required to execute an RPC. Each
/// concrete type of this class can create RPCs for a particular proxy type P. Clients of
/// this class should create an Rpc object using Make(). The Rpc class functions as a
/// builder for a single remote method invocation. Clients can set timeouts and retry
/// parameters, and then execute a remote method using either Thrift or ProtoBuf
/// arguments.
///
/// For example:
///
/// auto rpc = Rpc<MyServiceProxy>::Make(address, rpc_mgr)
///     .SetTimeout(timeout)
///     .SetRetryInterval(500);
/// RpcRequestPb request;
/// RpcResponsePb response;
/// RETURN_IF_ERROR(rpc.Execute(&MyServiceProxy::Rpc, request, &response));
///
/// For the async case, callers must make sure that the request and response objects are
/// deleted after the RPC completes:
///
/// unique_ptr<RpcRequestPB> request = make_unique<RpcRequestPB>();
/// unique_ptr<RpcResponsePB> response = make_unique<RpcResponsePB>();
///
/// // Release request and response into the completion handler, which is guaranteed to be
/// // called.
/// Rpc<MyServiceProxy>::Make(address, rpc_mgr)
///     .ExecuteAsync(&MyServiceProxy::Rpc, request.release(), response.release(),
///       [](const Status& status, RpcRequestPB* request, RpcResponsePB* response) {
///         if (!status.ok()) LOG(INFO) << "Error!";
///         delete request;
///         delete response;
///     });
///
/// All RPCs must have a timeout set. The default timeout is 5 minutes.

/// TODO: Move these into Rpc<>?
static constexpr int RPC_DEFAULT_MAX_ATTEMPTS = 3;
static constexpr int RPC_DEFAULT_RETRY_INTERVAL_MS = 100;
static constexpr int RPC_DEFAULT_TIMEOUT_S = 300;

template <typename P>
class Rpc {
 public:
  /// Factory method to create new Rpcs. 'remote' is the address of the machine on which
  /// the service to be called is running.
  static Rpc Make(const TNetworkAddress& remote, RpcMgr* mgr) {
    DCHECK(mgr != nullptr);
    DCHECK(mgr->is_inited()) << "Tried to build an RPC before RpcMgr::Init() is called";
    return Rpc(remote, mgr);
  }

  /// Sets the timeout for TCP writes and reads. If this timeout expires, Execute() will
  /// return an rpc-layer error.
  Rpc& SetTimeout(kudu::MonoDelta rpc_timeout) {
    params_->rpc_timeout = rpc_timeout;
    return *this;
  }

  /// Sets the maximum number of attempts for retry when the remote service is too busy.
  Rpc& SetMaxAttempts(int32_t max_attempts) {
    DCHECK_LE(1, max_attempts);
    params_->max_rpc_attempts = max_attempts;
    return *this;
  }

  /// Sets the maximum number of attempts for retry when the remote service is too busy.
  Rpc& SetRetryInterval(int32_t interval_ms) {
    DCHECK_LT(0, interval_ms);
    params_->retry_interval_ms = kudu::MonoDelta::FromMilliseconds(interval_ms);
    return *this;
  }

  // Adds an outbound sidecar to the list of sidecars to be sent along with the RPC
  // request. 'idx' is set to the index of 'sidecar' in the outbound list. Ownership is
  // shared by the caller, and the RPC subsystem. The RPC subsystem may read from the
  // sidecar's buffer even after the RPC has finished (see KUDU-2011); sharing ownership
  // makes it easy to avoid use-after-free bugs.
  Rpc& AddSidecar(std::shared_ptr<kudu::faststring> sidecar, int* idx) {
    params_->outbound_sidecars.push_back(std::move(sidecar));
    *idx = params_->outbound_sidecars.size() - 1;
    return *this;
  }

  // Fills 'sidecar' with index 'idx' with the slice that represents a sidecar payload
  // returned after an Rpc has completed. If 'idx' is larger than the number of inbound
  // sidecars, an error is returned. This method may only be called after a synchronous
  // RPC invocation using Execute().
  Status GetInboundSidecar(int idx, kudu::Slice* sidecar) {
    DCHECK(controller_.get() != nullptr);
    return FromKuduStatus(controller_->GetInboundSidecar(idx, sidecar));
  }

  /// Executes this RPC asynchronously, retrying if necessary. The completion callback
  /// 'user_callback' is guaranteed to be called exactly once when the RPC completes -
  /// either after a successful invocation, or in the case of an error. The callback
  /// should have the following signature:
  ///
  /// user_callback(const Status& status, REQ* req, RESP* resp, RpcController* controller)
  ///
  /// where
  ///   'status' is OK if the RPC was successfully attempted (i.e. there were no
  ///   problems acquiring the resources to send the RPC)
  ///   'req' and 'resp' are the protobuf messages passed into ExecuteAsync().
  ///   'controller' is the KRPC RpcController which contains the status of the RPC
  ///   attempt. If !status.ok(), may be nullptr.
  ///
  /// 'user_callback' will be called from a reactor thread, which are a finite resource
  /// that do much of the work of RPC processing. Therefore, 'user_callback' should not
  /// block for an extended period of time, and should do as little work as possible.
  ///
  /// The RPC parameter objects 'req' and 'resp' are owned by the caller, and must not be
  /// destroyed before 'user_callback' is called. A convenient pattern is to allocate
  /// 'req' and 'resp' on the heap and delete them in 'user_callback'.
  template <typename RPCMETHOD, typename REQ, typename RESP, typename CALLBACKB>
  void ExecuteAsync(
      const RPCMETHOD& rpc_method, REQ* req, RESP* resp, const CALLBACKB& user_callback) {
    Status status = CheckConfiguration();
    if (!status.ok()) {
      user_callback(status, req, resp, nullptr);
      return;
    }
    ExecuteAsyncHelper(rpc_method, req, resp, 0, user_callback, std::move(params_), mgr_);
  }

  /// Executes this RPC. If the remote service is too busy, execution is re-attempted up
  /// to a fixed number of times, after which an error is returned. Retries are attempted
  /// only if the remote server signals that it is too busy. Retries are spaced by the
  /// configured retry interval. All return values are the RPC-layer status; if OK() then
  /// the RPC was successfully executed. Otherwise, the remote service indicated an
  /// RPC-level failure. Application-level failures should be returned as Protobuf member
  /// fields.
  ///
  /// The actual method to invoke is passed in 'rpc_method' and is of type 'RPC_METHOD',
  /// and is typically a member function pointer, e.g.:
  ///
  /// rpc.Execute(&MyServiceProxy::SomeRpc, request, &response);
  ///
  /// Therefore F is the type of a synchronous RPC method, with signature:
  ///
  /// func(const REQ& request, RESP* response, RpcController* controller).
  template <typename RPCMETHOD, typename REQ, typename RESP>
  Status Execute(const RPCMETHOD& rpc_method, const REQ& req, RESP* resp) {
    RETURN_IF_ERROR(CheckConfiguration());
    std::unique_ptr<P> proxy;
    RETURN_IF_ERROR(mgr_->GetProxy(params_->remote, &proxy));
    controller_.reset(new kudu::rpc::RpcController());
    for (int i = 0; i < params_->max_rpc_attempts; ++i) {
      RETURN_IF_ERROR(InitController(controller_.get(), *params_));
      ((proxy.get())->*rpc_method)(req, resp, controller_.get());
      if (controller_->status().ok()) return Status::OK();

      // Retry only if the remote is too busy. Otherwise we fail fast.
      if (!IsRetryableError(*controller_)) return FromKuduStatus(controller_->status());

      SleepForMs(params_->retry_interval_ms.ToMilliseconds());
    }

    return FromKuduStatus(controller_->status());
  }

  /// Wrapper for Execute() that handles serialization from and to Thrift
  /// arguments. Provided for compatibility with RPCs that have not yet been translated to
  /// native Protobuf. Returns an error if serialization to or from protobuf fails,
  /// otherwise returns the same as Execute().
  template <typename RPCMETHOD, typename TREQ, typename TRESP>
  Status ExecuteWithThriftArgs(const RPCMETHOD& rpc_method, TREQ* req, TRESP* resp) {
    ThriftWrapperPb request_proto;
    auto serialized = std::make_shared<kudu::faststring>();
    ThriftSerializer serializer(true);
    RETURN_IF_ERROR(serializer.Serialize(req, serialized.get()));
    int idx = -1;
    AddSidecar(serialized, &idx);
    request_proto.set_sidecar_idx(idx);

    ThriftWrapperPb response_proto;
    RETURN_IF_ERROR(Execute(rpc_method, request_proto, &response_proto));
    kudu::Slice sidecar;
    RETURN_IF_ERROR(GetInboundSidecar(response_proto.sidecar_idx(), &sidecar));

    uint32_t len = sidecar.size();
    RETURN_IF_ERROR(DeserializeThriftMsg(sidecar.data(), &len, true, resp));
    return Status::OK();
  }

 private:
  struct RpcParams {
    RpcParams() { }

    /// All RPCs must have a valid timeout. The default is five minutes, which should
    /// greatly exceed the maximum RPC processing time.
    kudu::MonoDelta rpc_timeout = kudu::MonoDelta::FromSeconds(RPC_DEFAULT_TIMEOUT_S);

    /// The maximum number of retries for this RPC before an error is returned to the
    /// caller.
    int32_t max_rpc_attempts = RPC_DEFAULT_MAX_ATTEMPTS;

    /// The time, in ms, between retry attempts.
    kudu::MonoDelta retry_interval_ms =
        kudu::MonoDelta::FromMilliseconds(RPC_DEFAULT_RETRY_INTERVAL_MS);

    /// The address of the remote machine to send the RPC to.
    TNetworkAddress remote;

    /// List of outbound sidecars that will be serialized after the request payload during
    /// Execute().
    std::vector<std::shared_ptr<kudu::faststring>> outbound_sidecars;
   private:
    /// Avoid accidentally copying this struct.
    DISALLOW_COPY_AND_ASSIGN(RpcParams);
  };

  // All parameters for this RPC, encapsulated so that they may be passed as one to
  // callback methods (see ExecuteAsyncHelper()).
  //
  // Wrapped in a shared_ptr<> so that the params may be moved between callbacks
  // efficiently. We may not use a unique_ptr<>, nor rely on move(params_), as lambdas
  // that are converted to function() objects must be copyable.
  //
  // Calling Execute() or ExecuteAsync() may move() params_, and so this field is only
  // valid up until the RPC is executed.
  std::shared_ptr<RpcParams> params_ = std::make_shared<RpcParams>();

  // Rpc controller storage. Used only for synchronous RPCs so that the caller can access
  // sidecar memory after the RPC returns. For asynchronous RPCs the caller is called
  // with the controller as an argument.
  std::unique_ptr<kudu::rpc::RpcController> controller_;

  /// The RpcMgr handling this RPC.
  RpcMgr* mgr_ = nullptr;

  Rpc(const TNetworkAddress& remote, RpcMgr* mgr) : mgr_(mgr) {
    params_->remote = remote;
  }

  Status CheckConfiguration() const {
    if (params_->max_rpc_attempts < 1) {
      return Status(strings::Substitute("Invalid number of retry attempts: $0",
          params_->max_rpc_attempts));
    }

    if (params_->max_rpc_attempts > 1 &&
        params_->retry_interval_ms.ToMilliseconds() <= 0) {
      return Status(strings::Substitute("Invalid retry interval: $0",
          params_->retry_interval_ms.ToMilliseconds()));
    }

    return Status::OK();
  }

  /// Returns true if the controller is in an error state that corresponds to the remote
  /// server being too busy to handle this request. In that case, we may want to retry
  /// after waiting.
  static bool IsRetryableError(const kudu::rpc::RpcController& controller) {
    const kudu::rpc::ErrorStatusPB* err = controller.error_response();
    return controller.status().IsRemoteError() && err && err->has_code()
        && err->code() == kudu::rpc::ErrorStatusPB::ERROR_SERVER_TOO_BUSY;
  }

  /// Reset 'controller' to a state ready to send this RPC.
  static Status InitController(kudu::rpc::RpcController* controller,
      const RpcParams& params) {
    controller->Reset();
    controller->set_timeout(params.rpc_timeout);
    for (const auto& sidecar : params.outbound_sidecars) {
      int dummy;
      RETURN_IF_ERROR(FromKuduStatus(controller->AddOutboundSidecar(
          kudu::rpc::RpcSidecar::FromSharedPtr(sidecar), &dummy)));
    }
    return Status::OK();
  }

  /// Helper method for ExecuteAsync(). Implements retry logic by scheduling a retry task
  /// on a reactor thread.
  template <typename RPCMETHOD, typename REQ, typename RESP, typename CALLBACK>
  static void ExecuteAsyncHelper(const RPCMETHOD& rpc_method, REQ* req, RESP* resp,
      int num_attempts, CALLBACK user_callback, std::shared_ptr<RpcParams> params,
      RpcMgr* mgr) {
    using RpcController = kudu::rpc::RpcController;
    std::unique_ptr<RpcController> controller = std::make_unique<RpcController>();
    RpcController* controller_ptr = controller.get();
    Status status = InitController(controller_ptr, *params);

    ++num_attempts;
    std::unique_ptr<P> proxy;
    if (status.ok()) status = mgr->GetProxy(params->remote, &proxy);
    if (!status.ok()) {
      user_callback(status, req, resp, nullptr);
      return;
    }

    // Wraps the supplied user callback to implement retry logic for asynchronous
    // RPCs. May be executed after the enclosing ExecuteAsyncHelper() call has finished,
    // and indeed after the enclosing Rpc object has been destroyed. Therefore capture by
    // value all state necessary to retry the RPC.
    //
    // The completion callback 'user_callback' is guaranteed to be called.
    //
    // Retries are scheduled on a reactor thread. Since this callback is also called from
    // a reactor thread, we cannot sleep here before rescheduling (like we do in the
    // synchronous case).
    auto user_cb_wrapper = [params = std::move(params), mgr, rpc_method, req, resp,
        num_attempts, user_callback = std::move(user_callback),
        controller_ptr = controller.release()]() mutable {
      // Ensure that controller is always deleted on function exit.
      std::unique_ptr<RpcController> controller(controller_ptr);

      // If this RPC should not be retried (either because of success or some
      // unrecoverable error), call the completion callback.
      if (!IsRetryableError(*controller_ptr) ||
          num_attempts >= params->max_rpc_attempts) {
        // First status argument indicates no failure in actually building and passing the
        // RPC to the KRPC layer.
        user_callback(Status::OK(), req, resp, controller_ptr);
        return;
      }

      // Create a new task that retries the execution in params->retry_interval_ms
      // milliseconds, executed on the reactor thread.
      kudu::MonoDelta retry_interval = params->retry_interval_ms;
      auto retry_task = [params = std::move(params), mgr, rpc_method, req, resp,
          num_attempts, user_callback = std::move(user_callback)]
          (const kudu::Status& status) mutable {
        // Here 'status' refers to the success of scheduling on the reactor thread itself,
        // which typically does not fail.
        if (!status.ok()) {
          user_callback(FromKuduStatus(status), req, resp, nullptr);
          return;
        }

        // Call the async helper method to start the whole process again.
        ExecuteAsyncHelper(
            rpc_method, req, resp, num_attempts, user_callback, std::move(params), mgr);
      };
      mgr->messenger()->ScheduleOnReactor(retry_task, retry_interval);
    };

    ((proxy.get())->*rpc_method)(*req, resp, controller_ptr, user_cb_wrapper);
  }
};

template <typename T>
Status DeserializeFromSidecar(kudu::rpc::RpcContext* context, int idx, T* output) {
  kudu::Slice slice;
  RETURN_IF_ERROR(FromKuduStatus(context->GetInboundSidecar(idx, &slice)));
  uint32_t len = slice.size();
  return DeserializeThriftMsg(slice.data(), &len, true, output);
}

template <typename T>
Status SerializeToSidecar(
    kudu::rpc::RpcContext* context, T* input, ThriftWrapperPb* container) {
  ThriftSerializer serializer(true);
  std::unique_ptr<kudu::faststring> buffer(new kudu::faststring());
  RETURN_IF_ERROR(serializer.Serialize(input, buffer.get()));
  int idx;
  RETURN_IF_ERROR(FromKuduStatus(context->AddOutboundSidecar(
              kudu::rpc::RpcSidecar::FromFaststring(std::move(buffer)), &idx)));
  container->set_sidecar_idx(idx);
  return Status::OK();
}

}

#endif
