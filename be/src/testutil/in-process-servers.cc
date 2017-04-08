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

#include "testutil/in-process-servers.h"

#include <boost/scoped_ptr.hpp>
#include <stdlib.h>

#include "rpc/thrift-server.h"
#include "rpc/thrift-util.h"
#include "runtime/exec-env.h"
#include "service/impala-internal-service.h"
#include "service/impala-server.h"
#include "util/default-path-handlers.h"
#include "util/metrics.h"
#include "util/network-util.h"
#include "util/webserver.h"

#include "common/names.h"

DECLARE_string(ssl_server_certificate);
DECLARE_string(ssl_private_key);
DECLARE_int32(be_port);
DECLARE_int32(state_store_subscriber_port);

using namespace apache::thrift;
using namespace impala;

InProcessImpalaServer* InProcessImpalaServer::StartWithEphemeralPorts(
    const string& statestore_host, int statestore_port) {
  for (int tries = 0; tries < 10; ++tries) {
    vector<int> used_ports;
    int backend_port = FindUnusedEphemeralPort(&used_ports);
    if (backend_port == -1) continue;
    // This flag is read directly in several places to find the address of the local
    // backend interface.
    FLAGS_be_port = backend_port;

    int webserver_port = FindUnusedEphemeralPort(&used_ports);
    if (webserver_port == -1) continue;

    int beeswax_port = FindUnusedEphemeralPort(&used_ports);
    if (beeswax_port == -1) continue;

    int hs2_port = FindUnusedEphemeralPort(&used_ports);
    if (hs2_port == -1) continue;

    int data_svc_port = FindUnusedEphemeralPort(&used_ports);
    if (data_svc_port == -1) continue;

    InProcessImpalaServer* impala = new InProcessImpalaServer("localhost", backend_port,
        data_svc_port, webserver_port, statestore_host, statestore_port);
    // Start the daemon and check if it works, if not delete the current server object and
    // pick a new set of ports
    Status started = impala->StartWithClientServers(beeswax_port, hs2_port);
    if (started.ok()) {
      impala->SetCatalogInitialized();
      return impala;
    }
    LOG(INFO) << "Failed to start Impala server: " << started.GetDetail();
    impala->Shutdown();
    impala->Join();
    delete impala;
  }
  DCHECK(false) << "Could not find port to start Impalad.";
  return NULL;
}

InProcessImpalaServer::InProcessImpalaServer(const string& hostname, int backend_port,
    int data_svc_port, int webserver_port, const string& statestore_host, int statestore_port)
  : hostname_(hostname),
    backend_port_(backend_port),
    beeswax_port_(0),
    hs2_port_(0),
    impala_server_(NULL),
    exec_env_(new ExecEnv(hostname, backend_port, data_svc_port, webserver_port,
            statestore_host, statestore_port)) {}

void InProcessImpalaServer::SetCatalogInitialized() {
  DCHECK(impala_server_ != NULL) << "Call Start*() first.";
  exec_env_->frontend()->SetCatalogInitialized();
}

Status InProcessImpalaServer::StartWithClientServers(int beeswax_port, int hs2_port) {
  beeswax_port_ = beeswax_port;
  hs2_port_ = hs2_port;

  exec_env_->Init();
  impala_server_.reset(new ImpalaServer(exec_env_.get()));
  RETURN_IF_ERROR(impala_server_->Init(beeswax_port, hs2_port));
  RETURN_IF_ERROR(impala_server_->Start());

  return Status::OK();
}

void InProcessImpalaServer::Shutdown() {
  impala_server_->Shutdown();
}

void InProcessImpalaServer::Join() {
  impala_server_->Join();
}
