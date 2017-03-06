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

#ifndef IMPALA_SERVICE_AUTHENTICATION_EXPORT_H
#define IMPALA_SERVICE_AUTHENTICATION_EXPORT_H

#include <string>

namespace impala {

/// This file contains funcitons that are used by the KuduRPC code. To avoid conflicts
/// between Impala's utils and KuduRPC's utils, this file was created and is included by
/// KuduRPC's code.

// Authorizes authenticated users on an internal connection after validating that the
// first components of the 'requested_user' and our principal are the same.
int ImpalaSaslAuthorizeInternal(sasl_conn_t* conn, void* context,
    const char* requested_user, unsigned rlen,
    const char* auth_identity, unsigned alen,
    const char* def_realm, unsigned urlen,
    struct propctx* propctx);

// Returns the registered service name that SASL needs to communicate with its peers.
// Should only be called if Kerberos is enabled.
std::string GetSaslProtoName();

// Returns the Kerberos realm that Impala is operating under.
// Should only be called if Kerberos is enabled.
std::string GetKerberosRealm();

}
#endif // IMPALA_SERVICE_AUTHENTICATION_EXPORT_H
