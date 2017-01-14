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
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/statestore")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/statestore")

KRPC_GENERATE(STATESTORE_SVC_PROTO_SRCS STATESTORE_SVC_PROTO_HDRS
  STATESTORE_SVC_PROTO_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}
  EXTRA_PROTO_PATHS ${CMAKE_CURRENT_SOURCE_DIR}/../
  PROTO_FILES statestore.proto)

add_custom_target(statestore-proto-deps DEPENDS ${STATESTORE_SVC_PROTO_TGTS})

add_library(Statestore
  failure-detector.cc
  ${STATESTORE_SVC_PROTO_SRCS}
  statestore.cc
  statestore-subscriber.cc
  statestored-main.cc
)
add_dependencies(Statestore generated-deps)

ADD_BE_TEST(statestore-test)
