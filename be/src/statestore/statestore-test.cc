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

#include "common/atomic.h"
#include "common/init.h"
#include "rpc/rpc.h"
#include "statestore/statestore-subscriber.h"
#include "statestore/statestore.proxy.h"
#include "statestore/statestore.service.h"
#include "testutil/gtest-util.h"
#include "util/metrics.h"
#include "util/webserver.h"

#include "common/names.h"

using namespace impala;
using kudu::rpc::RpcContext;

DECLARE_string(ssl_server_certificate);
DECLARE_string(ssl_private_key);
DECLARE_string(ssl_client_ca_certificate);

DECLARE_int32(webserver_port);
DECLARE_int32(state_store_port);
DECLARE_int32(statestore_update_frequency_ms);
DECLARE_int32(statestore_heartbeat_frequency_ms);
DECLARE_int32(statestore_heartbeat_tcp_timeout_seconds);

namespace impala {

/// Subscriber implementation that allows tests to inject their own logic into the
/// heartbeat and update state rpc handlers.
class TestSubscriber : public StatestoreSubscriberIf {
 public:
  TestSubscriber(int id, RpcMgr* rpc_mgr, const TNetworkAddress& statestore_address, int32_t port)
    : StatestoreSubscriberIf(rpc_mgr->metric_entity(), rpc_mgr->result_tracker()),
      rpc_mgr_(rpc_mgr),
      statestore_address_(statestore_address),
      port_(port) {
    subscriber_id_ = Substitute("statestore-test-subscriber-$0", id);
  }

  virtual void UpdateState(
      const ThriftWrapperPb* request, ThriftWrapperPb* response, RpcContext* context) {
    TUpdateStateRequest thrift_request;
    Status status =
        DeserializeFromSidecar(context, response->sidecar_idx(), &thrift_request);

    TUpdateStateResponse thrift_response;
    update_state_count_.Add(1);
    if (update_state_cb_) update_state_cb_(this, &thrift_request, &thrift_response);

    status.ToThrift(&thrift_response.status);
    SerializeToSidecar(context, &thrift_response, response);

    context->RespondSuccess();
  }

  virtual void Heartbeat(
      const ThriftWrapperPb* request, ThriftWrapperPb* response, RpcContext* context) {
    THeartbeatRequest thrift_request;
    Status status = DeserializeFromSidecar(context, response->sidecar_idx(), &thrift_request);
    heartbeat_count_.Add(1);

    THeartbeatResponse thrift_response;
    if (heartbeat_cb_) heartbeat_cb_(this, &thrift_request, &thrift_response);

    SerializeToSidecar(context, &thrift_response, response);
    context->RespondSuccess();
  }

  typedef std::function<void(TestSubscriber*, THeartbeatRequest*, THeartbeatResponse*)>
  HeartbeatCallback;
  typedef std::function<
    void(TestSubscriber*, TUpdateStateRequest*, TUpdateStateResponse*)>
      UpdateStateCallback;

  void SetHeartbeatCallback(const HeartbeatCallback& cb) { heartbeat_cb_ = cb; }
  void SetUpdateStateCallback(const UpdateStateCallback& cb) { update_state_cb_ = cb; }

  Status Register(vector<TTopicRegistration> topics = {}) {
    TRegisterSubscriberRequest request;
    request.__set_subscriber_id(subscriber_id_);
    request.__set_subscriber_location(MakeNetworkAddress("localhost", port_));
    request.__set_topic_registrations(topics);

    TRegisterSubscriberResponse response;
    RETURN_IF_ERROR(Rpc<StatestoreServiceProxy>::Make(statestore_address_, rpc_mgr_)
        .ExecuteWithThriftArgs(
            &StatestoreServiceProxy::RegisterSubscriber, &request, &response));

    registration_id_ = response.registration_id;
    return Status::OK();
  }

  void WaitForHeartbeats(int count) {
    while (heartbeat_count_.Load() < count) SleepForMs(50);
  }

  void WaitForUpdates(int count) {
    while (update_state_count_.Load() < count) SleepForMs(50);
  }

  int update_state_count() { return update_state_count_.Load(); }
  const TUniqueId& registration_id() { return registration_id_; }
  RpcMgr* rpc_mgr() { return rpc_mgr_; }

  // Terminate the subscriber service, so the statestore will not be able to deliver
  // heartbeats or updates.
  void Kill() { rpc_mgr()->UnregisterServices(); }

  // Wait until the statestore no longer shows our subscriber ID in the list of registered
  // subscribers.
  void WaitForFailure(Statestore* statestore) {
    while (true) {
      set<Statestore::SubscriberId> ids = statestore->GetActiveSubscribersForTesting();
      if (ids.find(subscriber_id_) == ids.end()) return;
      SleepForMs(50);
    }
  }

 private:
  // Subscriber ID, set by us.
  string subscriber_id_;

  // Registration ID, returned by statestore.
  TUniqueId registration_id_;

  RpcMgr* rpc_mgr_;
  TNetworkAddress statestore_address_;
  int32_t port_;

  AtomicInt32 heartbeat_count_;
  AtomicInt32 update_state_count_;

  HeartbeatCallback heartbeat_cb_;
  UpdateStateCallback update_state_cb_;
};

class StatestoreTest : public testing::Test {
 protected:
  unique_ptr<Statestore> statestore_;
  unique_ptr<MetricGroup> metrics_;
  unique_ptr<Webserver> webserver_;
  vector<TestSubscriber*> subscribers_;
  vector<unique_ptr<RpcMgr>> rpc_mgrs_;
  TNetworkAddress resolved_localhost_;

  virtual void SetUp() {
    // Speed up test execution by sending messages very frequently.
    FLAGS_statestore_heartbeat_frequency_ms = 25;
    FLAGS_statestore_update_frequency_ms = 25;
    FLAGS_state_store_port = FindUnusedEphemeralPort(nullptr);
    ASSERT_NE(-1, FLAGS_state_store_port);

    ASSERT_OK(ResolveAddr(
        MakeNetworkAddress("localhost", FLAGS_state_store_port), &resolved_localhost_));

    // Reduce timeout for heartbeats
    FLAGS_statestore_heartbeat_tcp_timeout_seconds = 1;

    int webserver_port = FindUnusedEphemeralPort(nullptr);
    ASSERT_NE(-1, webserver_port);
    metrics_.reset(new MetricGroup("foo"));
    webserver_.reset(new Webserver(webserver_port));
    webserver_->Start();
    StartThreadInstrumentation(metrics_.get(), webserver_.get(), false);
    statestore_.reset(new Statestore(metrics_.get()));
    statestore_->RegisterWebpages(webserver_.get());
    statestore_->Start();
  }

  void InitSubscribers(int count) {
    vector<TestSubscriber*> subscribers;
    for (int i = 0; i < count; ++i) {
      rpc_mgrs_.push_back(make_unique<RpcMgr>());
      ASSERT_OK(rpc_mgrs_.back()->Init(1));
      int port = FindUnusedEphemeralPort(nullptr);
      ASSERT_NE(-1, port) << "Could not find unused ephemeral port!";
      unique_ptr<TestSubscriber> subscriber = make_unique<TestSubscriber>(i,
          rpc_mgrs_.back().get(), resolved_localhost_, port);
      subscribers_.push_back(subscriber.get());
      ASSERT_OK(subscribers_.back()->rpc_mgr()->RegisterService(2, 10, move(subscriber)));
      ASSERT_OK(subscribers_.back()->rpc_mgr()->StartServices(port, 1));
    }
  }

  virtual void TearDown() {
    for (auto sub : subscribers_) sub->rpc_mgr()->UnregisterServices();
    statestore_->SetExitFlag();
    statestore_->Join();
  }

  static TTopicDelta MakeTopicUpdate(const string& topic_name) {
    TTopicItem item;
    item.__set_key("foo1");
    item.__set_value("bar1");
    TTopicDelta delta;
    delta.__set_topic_name(topic_name);
    delta.__set_topic_entries({item});
    delta.__set_topic_deletions({});
    delta.__set_is_delta(false);
    return delta;
  }
};

TEST_F(StatestoreTest, SmokeTest) {
  RpcMgr rpc_mgr;
  rpc_mgr.Init(1);
  StatestoreSubscriber sub_will_start("sub1",
      MakeNetworkAddress(resolved_localhost_.hostname, FLAGS_state_store_port + 10),
      resolved_localhost_, &rpc_mgr, metrics_.get());

  ASSERT_OK(sub_will_start.Start());
  ASSERT_OK(rpc_mgr.StartServices(FLAGS_state_store_port + 10, 2));

  int64_t now = MonotonicMillis();
  while (
      sub_will_start.num_heartbeats_received() < 3 && (MonotonicMillis() - now < 10000)) {
    SleepForMs(100);
  }

  ASSERT_GE(sub_will_start.num_heartbeats_received(), 3)
      << "Only received " << sub_will_start.num_heartbeats_received() << " heartbeats";

  sub_will_start.Shutdown();

  rpc_mgr.UnregisterServices();

  // TODO(KRPC): SSL test
}

TEST_F(StatestoreTest, SubscriberSmokeTest) {
  InitSubscribers(1);
  ASSERT_OK(subscribers_[0]->Register());
}

TEST_F(StatestoreTest, RegistrationIdsDifferent) {
  // Test that two different registrations from the same subscriber yield different
  // registration IDs.
  InitSubscribers(1);
  ASSERT_OK(subscribers_[0]->Register());
  TUniqueId reg_id = subscribers_[0]->registration_id();
  ASSERT_OK(subscribers_[0]->Register());
  ASSERT_NE(reg_id, subscribers_[0]->registration_id());
}

TEST_F(StatestoreTest, WaitForHeartbeats) {
  InitSubscribers(1);
  ASSERT_OK(subscribers_[0]->Register());
  subscribers_[0]->WaitForHeartbeats(5);
}

TEST_F(StatestoreTest, ReceiveUpdates) {
  string topic_name = "my_topic_name";

  auto cb = [topic_name](
      TestSubscriber* sub, TUpdateStateRequest* request, TUpdateStateResponse* response) {
    TTopicDelta delta = MakeTopicUpdate(topic_name);
    if (sub->update_state_count() == 1) {
      Status::OK().ToThrift(&response->status);
      response->__set_topic_updates({delta});
      response->__set_skipped(false);
      return;
    }

    if (sub->update_state_count() == 2) {
      ASSERT_EQ(1, request->topic_deltas.size());
      TTopicDelta d = request->topic_deltas[topic_name];
      ASSERT_EQ(d.topic_entries, delta.topic_entries);
      ASSERT_EQ(d.topic_name, delta.topic_name);
      ASSERT_EQ(d.topic_deletions, delta.topic_deletions);
      return;
    }

    if (sub->update_state_count() == 3) {
      ASSERT_EQ(0, request->topic_deltas[topic_name].topic_entries.size());
      ASSERT_EQ(0, request->topic_deltas[topic_name].topic_deletions.size());
    }
  };

  InitSubscribers(1);
  subscribers_[0]->SetUpdateStateCallback(cb);
  TTopicRegistration reg;
  reg.__set_topic_name(topic_name);
  reg.__set_is_transient(false);
  ASSERT_OK(subscribers_[0]->Register({reg}));
  subscribers_[0]->WaitForUpdates(4);
}

TEST_F(StatestoreTest, UpdateIsDelta) {
  string topic_name = "update_is_delta";

  auto cb = [topic_name](
      TestSubscriber* sub, TUpdateStateRequest* request, TUpdateStateResponse* response) {
    ASSERT_TRUE(request->topic_deltas.find(topic_name) != request->topic_deltas.end());
    if (sub->update_state_count() == 1) {
      ASSERT_FALSE(request->topic_deltas[topic_name].is_delta);
      TTopicDelta delta = MakeTopicUpdate(topic_name);
      Status::OK().ToThrift(&response->status);
      response->__set_topic_updates({delta});
      response->__set_skipped(false);
      return;
    }

    if (sub->update_state_count() == 2) {
      ASSERT_FALSE(request->topic_deltas[topic_name].is_delta);
      return;
    }

    if (sub->update_state_count() == 3) {
      ASSERT_TRUE(request->topic_deltas[topic_name].is_delta);
      ASSERT_EQ(0, request->topic_deltas[topic_name].topic_entries.size());
      ASSERT_EQ(1, request->topic_deltas[topic_name].to_version);
    }
  };

  InitSubscribers(1);
  subscribers_[0]->SetUpdateStateCallback(cb);
  TTopicRegistration reg;
  reg.__set_topic_name(topic_name);
  reg.__set_is_transient(false);
  ASSERT_OK(subscribers_[0]->Register({reg}));
  subscribers_[0]->WaitForUpdates(4);
}

TEST_F(StatestoreTest, UpdateSkipping) {
  // Test that skipping an update causes it to be resent
  string topic_name = "update_skipping";

  auto check_skipped = [topic_name](
      TestSubscriber* sub, TUpdateStateRequest* request, TUpdateStateResponse* response) {
    if (sub->update_state_count() == 1) {
      TTopicDelta delta = MakeTopicUpdate(topic_name);
      response->__set_topic_updates({delta});
      response->__set_skipped(false);
      Status::OK().ToThrift(&response->status);
      return;
    }

    // All subsequent updates: set skipped = true and expect the full topic to be resent
    // every time.
    ASSERT_TRUE(request->topic_deltas.find(topic_name) != request->topic_deltas.end());
    ASSERT_EQ(1, request->topic_deltas[topic_name].topic_entries.size());
    Status::OK().ToThrift(&response->status);
    response->__set_skipped(true);
  };

  InitSubscribers(1);
  subscribers_[0]->SetUpdateStateCallback(check_skipped);
  TTopicRegistration reg;
  reg.__set_topic_name(topic_name);
  reg.__set_is_transient(false);
  ASSERT_OK(subscribers_[0]->Register({reg}));
  subscribers_[0]->WaitForUpdates(4);
}

TEST_F(StatestoreTest, FailureDetection) {
  InitSubscribers(1);
  subscribers_[0]->Register({});
  subscribers_[0]->WaitForUpdates(1);
  subscribers_[0]->Kill();
  subscribers_[0]->WaitForFailure(statestore_.get());
}

TEST_F(StatestoreTest, HungHeartbeat) {
  // Test for IMPALA-1712: If heartbeats hang the statestore should time them out.
  shared_ptr<Promise<bool>> failure_detected(new Promise<bool>());
  auto heartbeat_cb = [failure_detected](TestSubscriber* sub, THeartbeatRequest* request,
      THeartbeatResponse* response) { failure_detected->Get(); };
  InitSubscribers(1);
  subscribers_[0]->SetHeartbeatCallback(heartbeat_cb);
  ASSERT_OK(subscribers_[0]->Register({}));
  subscribers_[0]->WaitForFailure(statestore_.get());

  // Unblock the hung heartbeats.
  failure_detected->Set(true);
}

TEST_F(StatestoreTest, TopicPersistence) {
  string persistent_topic_name = "persistent_topic";
  string transient_topic_name = "transient_topic";

  auto add_entries = [persistent_topic_name, transient_topic_name](
      TestSubscriber* sub, TUpdateStateRequest* request, TUpdateStateResponse* response) {
    if (sub->update_state_count() == 1) {
      Status::OK().ToThrift(&response->status);
      vector<TTopicDelta> updates = {
          MakeTopicUpdate(persistent_topic_name), MakeTopicUpdate(transient_topic_name)};
      response->__set_topic_updates(updates);
      response->__set_skipped(false);
    }
  };

  auto check_entries = [persistent_topic_name, transient_topic_name](
      TestSubscriber* sub, TUpdateStateRequest* request, TUpdateStateResponse* response) {
    if (sub->update_state_count() == 1) {
      // Check the transient topic exists, but has no entries.
      ASSERT_TRUE(request->topic_deltas.find(transient_topic_name)
          != request->topic_deltas.end());
      ASSERT_EQ(0, request->topic_deltas[transient_topic_name].topic_entries.size());

      // Check that the persistent topic has entries.
      ASSERT_TRUE(request->topic_deltas.find(persistent_topic_name)
          != request->topic_deltas.end());
      ASSERT_EQ(1, request->topic_deltas[persistent_topic_name].topic_entries.size());

      // Statestore should not send deletions when the update is not a delta, see
      // IMPALA-1891
      // ASSERT_EQ(0, request->topic_deltas[transient_topic_name].topic_deletions.size());
    }
  };

  InitSubscribers(2);
  subscribers_[0]->SetUpdateStateCallback(add_entries);

  TTopicRegistration persistent_reg;
  persistent_reg.__set_topic_name(persistent_topic_name);
  persistent_reg.__set_is_transient(false);
  TTopicRegistration transient_reg;
  transient_reg.__set_topic_name(transient_topic_name);
  transient_reg.__set_is_transient(true);
  vector<TTopicRegistration> registrations = {persistent_reg, transient_reg};

  ASSERT_OK(subscribers_[0]->Register(registrations));
  subscribers_[0]->WaitForUpdates(3);
  subscribers_[0]->Kill();
  subscribers_[0]->WaitForFailure(statestore_.get());

  subscribers_[1]->SetUpdateStateCallback(check_entries);
  ASSERT_OK(subscribers_[1]->Register(registrations));
  subscribers_[1]->WaitForUpdates(2);
}

TEST_F(StatestoreTest, VeryLargeTopic) {
  string topic_name = "very_large_topic";
  constexpr int SIZE = 1024 * 1024 * 100;
  constexpr int NUM_ENTRIES = 10;
  auto add_entries = [topic_name]
      (TestSubscriber* sub, TUpdateStateRequest* request, TUpdateStateResponse* response) {
    if (sub->update_state_count() == 1) {
      Status::OK().ToThrift(&response->status);
      vector<TTopicItem> items;
      for (int i = 0; i < NUM_ENTRIES; ++i) {
        TTopicItem item;
        item.__set_key(Substitute("1gb-$0", i));
        item.__set_value("");
        item.value.resize(SIZE);
        items.push_back(item);
      }
      TTopicDelta delta;
      delta.__set_topic_name(topic_name);
      delta.__set_topic_entries(items);
      delta.__set_topic_deletions({});
      delta.__set_is_delta(false);
      vector<TTopicDelta> deltas = {delta};
      response->__set_topic_updates(deltas);
      response->__set_skipped(false);
    }
  };

  auto check_entries = [topic_name]
      (TestSubscriber* sub, TUpdateStateRequest* request, TUpdateStateResponse* response) {
    if (sub->update_state_count() == 1) {
      ASSERT_TRUE(request->topic_deltas.find(topic_name) != request->topic_deltas.end());
      ASSERT_EQ(NUM_ENTRIES, request->topic_deltas[topic_name].topic_entries.size());
      for (int i = 0; i < NUM_ENTRIES; ++i) {
        const TTopicItem& item = request->topic_deltas[topic_name].topic_entries[i];
        ASSERT_EQ(SIZE, item.value.size());
      }
    }
  };

  InitSubscribers(2);
  subscribers_[0]->SetUpdateStateCallback(add_entries);
  subscribers_[1]->SetUpdateStateCallback(check_entries);
  TTopicRegistration reg;
  reg.__set_topic_name(topic_name);
  reg.__set_is_transient(true);
  vector<TTopicRegistration> regs = {reg};

  ASSERT_OK(subscribers_[0]->Register(regs));
  subscribers_[0]->WaitForUpdates(2);

  ASSERT_OK(subscribers_[1]->Register(regs));
  subscribers_[1]->WaitForUpdates(2);
}

}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, false, impala::TestInfo::BE_TEST);
  int rc = RUN_ALL_TESTS();
  // IMPALA-5291: statestore services and subscribers may still be running at this point
  // and accessing global state. Exit without running global destructors to avoid
  // races with other threads when tearing down the proces.
  _exit(rc);
}
