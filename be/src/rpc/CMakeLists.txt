# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/rpc")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/rpc")

PROTOBUF_GENERATE_CPP(
  COMMON_PROTO_SRCS COMMON_PROTO_HDRS COMMON_PROTO_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../
  PROTO_FILES common.proto)

add_custom_target(common-proto-deps DEPENDS ${COMMON_PROTO_TGTS})

add_library(Rpc
  authentication.cc
  ${COMMON_PROTO_SRCS}
  rpc-mgr.cc
  rpc-trace.cc
  TAcceptQueueServer.cpp
  thrift-util.cc
  thrift-client.cc
  thrift-server.cc
  thrift-thread.cc
  )

add_dependencies(Rpc generated-deps)

KRPC_GENERATE(RPC_TEST_SVC_PROTO_SRCS RPC_TEST_SVC_PROTO_HDRS
  RPC_TEST_SVC_PROTO_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}
  EXTRA_PROTO_PATHS ${CMAKE_CURRENT_BINARY_DIR}/../
  PROTO_FILES rpc_test.proto)

add_custom_target(rpc_test-proto-deps DEPENDS ${RPC_TEST_SVC_PROTO_TGTS})

ADD_BE_TEST(thrift-util-test)
ADD_BE_TEST(thrift-server-test)
ADD_BE_TEST(authentication-test)

ADD_BE_TEST(rpc-mgr-test)
ADD_DEPENDENCIES(rpc-mgr-test rpc_test-proto-deps)
add_library(rpc_test-proto ${RPC_TEST_SVC_PROTO_SRCS})
target_link_libraries(rpc-mgr-test rpc_test-proto)
