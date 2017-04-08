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

#include "service/impala-internal-service.h"

#include "common/status.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gutil/strings/substitute.h"
#include "kudu/rpc/rpc_context.h"
#include "rpc/rpc.h"
#include "rpc/thrift-util.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/exec-env.h"
#include "runtime/fragment-instance-state.h"
#include "runtime/query-exec-mgr.h"
#include "runtime/query-state.h"
#include "runtime/row-batch.h"
#include "service/impala-server.h"
#include "service/data_stream_service.pb.h"
#include "util/bloom-filter.h"
#include "testutil/fault-injection-util.h"

#include "common/names.h"

using namespace impala;
using kudu::rpc::RpcContext;

namespace impala {

DataStreamService::DataStreamService(RpcMgr* mgr)
  : DataStreamServiceIf(mgr->metric_entity(), mgr->result_tracker()) {}

void DataStreamService::EndDataStream(const EndDataStreamRequestPb* request,
    EndDataStreamResponsePb* response, RpcContext* context) {
  FAULT_INJECTION_RPC_DELAY(RPC_ENDDATASTREAM);
  TUniqueId finst_id;
  finst_id.__set_lo(request->dest_fragment_instance_id().lo());
  finst_id.__set_hi(request->dest_fragment_instance_id().hi());

  VLOG_ROW << "EndDataStream(): instance_id=" << PrintId(finst_id)
           << " node_id=" << request->dest_node_id()
           << " sender_id=" << request->sender_id();

  ExecEnv::GetInstance()->stream_mgr()->CloseSender(
      finst_id, request->dest_node_id(), request->sender_id());
  context->RespondSuccess();
}

void DataStreamService::TransmitData(const TransmitDataRequestPb* request,
    TransmitDataResponsePb* response, RpcContext* context) {
  FAULT_INJECTION_RPC_DELAY(RPC_TRANSMITDATA);
  TUniqueId finst_id;
  finst_id.__set_lo(request->dest_fragment_instance_id().lo());
  finst_id.__set_hi(request->dest_fragment_instance_id().hi());

  VLOG_ROW << "TransmitData(): instance_id=" << finst_id
           << " node_id=" << request->dest_node_id()
           << " #rows=" << request->row_batch_header().num_rows()
           << " sender_id=" << request->sender_id();
  InboundProtoRowBatch batch;
  Status status = FromKuduStatus(context->GetInboundSidecar(
      request->row_batch_header().tuple_data_sidecar_idx(), &batch.tuple_data));
  if (status.ok()) {
    status = FromKuduStatus(context->GetInboundSidecar(
        request->row_batch_header().tuple_offsets_sidecar_idx(), &batch.tuple_offsets));
  }
  if (status.ok()) {
    batch.header = request->row_batch_header();
    auto payload = make_unique<TransmitDataCtx>(batch, context, request, response);
    // AddData() is guaranteed to eventually respond to this RPC so we don't do it here.
    ExecEnv::GetInstance()->stream_mgr()->AddData(finst_id, move(payload));
  } else {
    // An application-level error occurred, so return 'success', but set the error status.
    status.ToProto(response->mutable_status());
    context->RespondSuccess();
  }
}

void DataStreamService::PublishFilter(const PublishFilterRequestPb* request,
    PublishFilterResponsePb* response, RpcContext* context) {
  TUniqueId query_id;
  query_id.__set_lo(request->query_id().lo());
  query_id.__set_hi(request->query_id().hi());

  QueryState::ScopedRef qs(query_id);
  if (qs.get() != nullptr) {
    ProtoBloomFilter proto_filter;
    proto_filter.header = request->bloom_filter();
    Status status;
    if (!proto_filter.header.always_true()) {
      int idx = proto_filter.header.directory_sidecar_idx();
      status = FromKuduStatus(context->GetInboundSidecar(idx, &proto_filter.directory));
    }
    if (status.ok()) {
      qs->PublishFilter(request->filter_id(), request->fragment_idx(), proto_filter);
    }
  }

  context->RespondSuccess();
}

void DataStreamService::UpdateFilter(const UpdateFilterRequestPb* request,
    UpdateFilterResponsePb* response, RpcContext* context) {
  TUniqueId query_id;
  query_id.lo = request->query_id().lo();
  query_id.hi = request->query_id().hi();

  ProtoBloomFilter filter;
  filter.header = request->bloom_filter();
  Status status = Status::OK();
  if (!filter.header.always_true()) {
    status = FromKuduStatus(context->GetInboundSidecar(
        filter.header.directory_sidecar_idx(), &filter.directory));
  }
  if (status.ok()) {
    ExecEnv::GetInstance()->impala_server()->UpdateFilter(
        request->filter_id(), query_id, filter);
  }
  context->RespondSuccess();
}

ImpalaInternalService::ImpalaInternalService() {
  impala_server_ = ExecEnv::GetInstance()->impala_server();
  DCHECK(impala_server_ != nullptr);
  query_exec_mgr_ = ExecEnv::GetInstance()->query_exec_mgr();
  DCHECK(query_exec_mgr_ != nullptr);
}

void ImpalaInternalService::ExecQueryFInstances(TExecQueryFInstancesResult& return_val,
    const TExecQueryFInstancesParams& params) {
  VLOG_QUERY << "ExecQueryFInstances():" << " query_id=" << params.query_ctx.query_id;
  FAULT_INJECTION_RPC_DELAY(RPC_EXECQUERYFINSTANCES);
  DCHECK(params.__isset.coord_state_idx);
  DCHECK(params.__isset.query_ctx);
  DCHECK(params.__isset.fragment_ctxs);
  DCHECK(params.__isset.fragment_instance_ctxs);
  query_exec_mgr_->StartQuery(params).SetTStatus(&return_val);
}

namespace {

template <typename T> void SetUnknownIdError(
    const string& id_type, const TUniqueId& id, T* status_container) {
  Status status(ErrorMsg(TErrorCode::INTERNAL_ERROR,
      Substitute("Unknown $0 id: $1", id_type, lexical_cast<string>(id))));
  status.SetTStatus(status_container);
}

}

void ImpalaInternalService::CancelQueryFInstances(
    TCancelQueryFInstancesResult& return_val,
    const TCancelQueryFInstancesParams& params) {
  VLOG_QUERY << "CancelQueryFInstances(): query_id=" << params.query_id;
  FAULT_INJECTION_RPC_DELAY(RPC_CANCELQUERYFINSTANCES);
  DCHECK(params.__isset.query_id);
  QueryState::ScopedRef qs(params.query_id);
  if (qs.get() == nullptr) {
    SetUnknownIdError("query", params.query_id, &return_val);
    return;
  }
  qs->Cancel();
}

void ImpalaInternalService::ReportExecStatus(TReportExecStatusResult& return_val,
    const TReportExecStatusParams& params) {
  FAULT_INJECTION_RPC_DELAY(RPC_REPORTEXECSTATUS);
  DCHECK(params.__isset.query_id);
  DCHECK(params.__isset.coord_state_idx);
  impala_server_->ReportExecStatus(return_val, params);
}

}
