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

#include "rpc/rpc.h"
#include "common/init.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "rpc/rpc-mgr.inline.h"
#include "rpc/rpc_test.proxy.h"
#include "rpc/rpc_test.service.h"
#include "testutil/gtest-util.h"
#include "util/counting-barrier.h"
#include "util/network-util.h"
#include "util/promise.h"

#include <functional>

#include "common/names.h"

using namespace impala;

using kudu::rpc::ServiceIf;
using kudu::rpc::RpcController;
using kudu::rpc::RpcContext;
using kudu::rpc::ErrorStatusPB;

using namespace std;

DECLARE_int32(num_reactor_threads);

namespace impala {

static int32_t SERVICE_PORT = FindUnusedEphemeralPort();

class RpcTest : public testing::Test {
 protected:
  RpcMgr rpc_mgr_;

  virtual void SetUp() { ASSERT_OK(rpc_mgr_.Init(FLAGS_num_reactor_threads)); }

  virtual void TearDown() { rpc_mgr_.UnregisterServices(); }
};

class PingServiceImpl : public PingServiceIf {
 public:
  // 'cb' is a callback used by tests to inject custom behaviour into the RPC handler.
  PingServiceImpl(const scoped_refptr<kudu::MetricEntity>& entity,
      const scoped_refptr<kudu::rpc::ResultTracker> tracker,
      std::function<void(RpcContext*)> cb =
          [](RpcContext* ctx) { ctx->RespondSuccess(); })
    : PingServiceIf(entity, tracker), cb_(cb) {}

  virtual void Ping(
      const PingRequestPb* request, PingResponsePb* response, RpcContext* context) {
    response->set_int_response(42);
    cb_(context);
  }

  virtual void PingThrift(
      const ThriftWrapperPb* request, ThriftWrapperPb* response, RpcContext* context) {
    TNetworkAddress req;
    DeserializeThriftFromProtoWrapper(*request, &req);

    // Do something to show the request was processed.
    req.port++;
    SerializeThriftToProtoWrapper(&req, response);
    cb_(context);
  }

 private:
  std::function<void(RpcContext*)> cb_;
};

TEST_F(RpcTest, ServiceSmokeTest) {
  // Test that a service can be started, and will respond to requests.
  unique_ptr<ServiceIf> impl(
      new PingServiceImpl(rpc_mgr_.metric_entity(), rpc_mgr_.result_tracker()));
  ASSERT_OK(rpc_mgr_.RegisterService(10, 1024, move(impl)));
  ASSERT_OK(rpc_mgr_.StartServices(SERVICE_PORT, 2));

  unique_ptr<PingServiceProxy> proxy;
  ASSERT_OK(rpc_mgr_.GetProxy<PingServiceProxy>(
      MakeNetworkAddress("localhost", SERVICE_PORT), &proxy));

  PingRequestPb request;
  PingResponsePb response;
  RpcController controller;
  proxy->Ping(request, &response, &controller);
  ASSERT_EQ(response.int_response(), 42);
  rpc_mgr_.UnregisterServices();
}

TEST_F(RpcTest, RetryPolicyTest) {
  // Test that retries happen the expected number of times.
  AtomicInt32 retries(0);
  auto cb = [&retries](RpcContext* context) {
    retries.Add(1);
    context->RespondRpcFailure(
        ErrorStatusPB::ERROR_SERVER_TOO_BUSY, kudu::Status::ServiceUnavailable(""));
  };

  unique_ptr<ServiceIf> impl(
      new PingServiceImpl(rpc_mgr_.metric_entity(), rpc_mgr_.result_tracker(), cb));
  ASSERT_OK(rpc_mgr_.RegisterService(10, 1024, move(impl)));
  rpc_mgr_.StartServices(SERVICE_PORT, 2);

  auto rpc = Rpc<PingServiceProxy>::Make(
      MakeNetworkAddress("localhost", SERVICE_PORT), &rpc_mgr_);

  PingRequestPb request;
  PingResponsePb response;
  ASSERT_FALSE(rpc.Execute(&PingServiceProxy::Ping, request, &response).ok());

  // Default
  ASSERT_EQ(RPC_DEFAULT_MAX_ATTEMPTS, retries.Load());

  retries.Store(0);
  rpc.SetMaxAttempts(10);
  ASSERT_FALSE(rpc.Execute(&PingServiceProxy::Ping, request, &response).ok());
  ASSERT_EQ(10, retries.Load());

  retries.Store(0);
  rpc.SetMaxAttempts(5).SetRetryInterval(200);
  int64_t now = MonotonicMillis();
  ASSERT_FALSE(rpc.Execute(&PingServiceProxy::Ping, request, &response).ok());
  ASSERT_GE(MonotonicMillis() - now, 5 * 200);

  rpc_mgr_.UnregisterServices();
}

TEST_F(RpcTest, TimeoutTest) {
  // Test that requests will timeout as configured if they take too long.
  auto cb = [](RpcContext* context) {
    SleepForMs(6000);
    context->RespondSuccess();
  };

  unique_ptr<ServiceIf> impl(
      new PingServiceImpl(rpc_mgr_.metric_entity(), rpc_mgr_.result_tracker(), cb));
  ASSERT_OK(rpc_mgr_.RegisterService(10, 1024, move(impl)));
  rpc_mgr_.StartServices(SERVICE_PORT, 2);

  auto rpc = Rpc<PingServiceProxy>::Make(
      MakeNetworkAddress("localhost", SERVICE_PORT), &rpc_mgr_);

  PingRequestPb request;
  PingResponsePb response;

  int64_t now = MonotonicMillis();
  Status status = rpc.SetTimeout(kudu::MonoDelta::FromSeconds(3))
      .Execute(&PingServiceProxy::Ping, request, &response);
  ASSERT_FALSE(status.ok());
  ASSERT_TRUE(status.GetDetail().find("timed out") != string::npos)
      << status.GetDetail();
  ASSERT_GE(MonotonicMillis() - now, 3000);

  rpc.SetTimeout(kudu::MonoDelta::FromSeconds(10));
  ASSERT_OK(rpc.Execute(&PingServiceProxy::Ping, request, &response));

  rpc_mgr_.UnregisterServices();
}

TEST_F(RpcTest, FullServiceQueueTest) {
  // Used to signal processing RPCs that they may complete.
  Promise<bool> latch;
  const int32_t NUM_SVC_THREADS = 10;
  const int32_t QUEUE_DEPTH = 5;
  const int32_t NUM_RPCS = NUM_SVC_THREADS + QUEUE_DEPTH;
  CountingBarrier barrier(NUM_RPCS);

  // At the end of the test, we want this == NUM_RPCS.
  AtomicInt32 num_rpcs_processed;

  auto cb = [&barrier, &latch, &num_rpcs_processed](RpcContext* context) {
    num_rpcs_processed.Add(1);
    // Wait until RPCs are allowed to complete.
    latch.Get();
    // Signal that this RPC is done.
    barrier.Notify();
  };

  unique_ptr<ServiceIf> impl(
      new PingServiceImpl(rpc_mgr_.metric_entity(), rpc_mgr_.result_tracker(), cb));
  ASSERT_OK(rpc_mgr_.RegisterService(NUM_SVC_THREADS, QUEUE_DEPTH, move(impl)));
  rpc_mgr_.StartServices(SERVICE_PORT, 2);

  const TNetworkAddress remote = MakeNetworkAddress("localhost", SERVICE_PORT);
  vector<unique_ptr<PingServiceProxy>> proxies;
  // Start NUM_RPCS RPCS concurrently so that they consume all service threads and then
  // fill the service queue.
  for (int i = 0; i < NUM_RPCS; ++i) {
    unique_ptr<PingServiceProxy> proxy;
    ASSERT_OK(rpc_mgr_.GetProxy<PingServiceProxy>(remote, &proxy));
    PingResponsePb* resp = new PingResponsePb();
    RpcController* controller = new RpcController();
    controller->set_timeout(kudu::MonoDelta::FromSeconds(60));
    proxy->PingAsync(PingRequestPb(), resp, controller,
        [resp, controller]() {
          delete resp;
          delete controller;
        });
    proxies.push_back(move(proxy));
    LOG(INFO) << "Started rpc number: " << i;
  }

  // Queue should be full. Try another RPC and check that it fails due to backpressure.
  auto rpc = Rpc<PingServiceProxy>::Make(
      MakeNetworkAddress("localhost", SERVICE_PORT), &rpc_mgr_)
      .SetTimeout(kudu::MonoDelta::FromSeconds(60))
      .SetRetryInterval(10)
      .SetMaxAttempts(10);

  PingRequestPb request;
  PingResponsePb response;

  Status status = rpc.Execute(&PingServiceProxy::Ping, request, &response);
  ASSERT_FALSE(status.ok());
  ASSERT_TRUE(
      status.GetDetail().find("dropped due to backpressure. The service queue is full")
      != string::npos) << status.GetDetail();

  LOG(INFO) << "Finished synchronous RPC";
  latch.Set(true);
  barrier.Wait();

  ASSERT_EQ(num_rpcs_processed.Load(), NUM_RPCS) << "More successful RPCs than expected";
}

TEST_F(RpcTest, ThriftWrapperTest) {
  int32_t port = 100;
  TNetworkAddress addr = MakeNetworkAddress("localhost", port);

  unique_ptr<ServiceIf> impl(
      new PingServiceImpl(rpc_mgr_.metric_entity(), rpc_mgr_.result_tracker()));
  ASSERT_OK(rpc_mgr_.RegisterService(1, 1, move(impl)));
  rpc_mgr_.StartServices(SERVICE_PORT, 2);

  auto rpc = Rpc<PingServiceProxy>::Make(
      MakeNetworkAddress("localhost", SERVICE_PORT), &rpc_mgr_);

  Status status = rpc.ExecuteWithThriftArgs(&PingServiceProxy::PingThrift, &addr, &addr);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(addr.port, port + 1);
}
}

IMPALA_TEST_MAIN();
