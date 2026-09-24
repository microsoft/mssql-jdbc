# OpenTelemetry for mssql-jdbc connection errors

**Total coverage: 123 error/condition entries across 16 categories**, referencing **129 distinct JDBC resource keys**, plus server error codes and JVM, OS, identity-provider, and external-pool failures. Some entries group related errors; 123 is the number of table entries, not a count of unique exception types or all possible runtime messages. Conditional rows are classified by their underlying evidence, not additional categories.

| Category | Errors covered |
|---|---|
| `name_resolution` | Host-name and DNS resolution failures. |
| `network_connectivity` | TCP connection failures, resets, unreachable networks, and unexpected transport closure. |
| `tls_security` | TLS negotiation, certificate validation, hostname verification, and ALPN failures. |
| `authentication` | SQL login rejection and integrated-authentication or token-acquisition failures. |
| `access_policy` | Explicit firewall, network-access, or identity-policy denials. |
| `routing_redirect` | Invalid routing information and excessive redirection. |
| `server_availability` | Server/database unavailability, throttling, and server-side resource limits. |
| `configuration` | Invalid or conflicting settings, missing credentials/dependencies, and incompatible runtime/setup. |
| `timeout` | Confirmed expiration during connection establishment, reads, TLS, routing, or token acquisition. |
| `canceled` | Explicit interruption or cancellation of connection/token acquisition. |
| `protocol_error` | Invalid prelogin, TDS, authentication metadata, or feature-negotiation responses. |
| `connection_lifecycle` | Closed or invalidated physical/logical connections and lifecycle-related failures. |
| `connection_recovery` | Session-recovery vetoes, missing acknowledgements, incompatible recovered state, and exhausted recovery attempts. |
| `client_resource_exhaustion` | Confirmed client memory, thread, socket/handle/port, or external connection-pool capacity exhaustion. |
| `internal_error` | Confirmed driver internal-state or timer-lifecycle invariant failures. |
| `unknown` | Unrecognized failures or insufficient evidence for a more specific classification. |

## Connection error mapping

| Error / resource key | Phase | Category | Exact message template or external-message status | Meaning / qualification | Source |
|---|---|---|---|---|---|
| `UnknownHostException` | DNS | `name_resolution` | Resolver/JDK-dependent message; no fixed JDBC template. | Required host cannot be resolved. TCP wrapping may discard the original type; do not infer DNS from a host string alone. | [SocketFinder](../src/main/java/com/microsoft/sqlserver/jdbc/IOBuffer.java#L2618-L2738) |
| `R_tcpipConnectionFailed` | TCP connect | `network_connectivity` | The TCP/IP connection to the host {0}, port {1} has failed. Error: "{2}". | Broad wrapper; not proof of firewall denial or server outage. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L79) |
| `R_tcpOpenFailed` | TCP connect | `network_connectivity` | {0}. Verify the connection properties. Make sure that an instance of SQL Server is running on the host and accepting TCP/IP connections at the port. Make sure that TCP connections to the port are not blocked by a firewall. | Embeds original exception message without preserving its type/cause. Firewall text is troubleshooting advice. | [Conversion](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerException.java#L437-L449) |
| Refused / reset / unreachable / broken pipe / aborted | TCP / transport | `network_connectivity` | OS/JDK-dependent text; no separate fixed JDBC template for each symptom. | Transport failure, not necessarily policy denial, server crash, or failed credentials. | [I/O](../src/main/java/com/microsoft/sqlserver/jdbc/IOBuffer.java#L2307-L2363) |
| `R_connectionTimedOut` | Connect / login budget | `timeout` | Connection timed out: no further information. | Used by socket finding when no specific exception is selected, and other timeout paths. Not every login budget expiration emits it. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L48) |
| `SocketTimeoutException` | Prelogin / login / TLS read | `timeout` | JDK-dependent, often `Read timed out`. | Read termination retains cause and uses internal code 8. Socket timeout is capped at setup, not continually recomputed as an aggregate deadline. | [Read](../src/main/java/com/microsoft/sqlserver/jdbc/IOBuffer.java#L2307-L2328), [setup](../src/main/java/com/microsoft/sqlserver/jdbc/IOBuffer.java#L758-L771) |
| `R_sqlBrowserFailed` | SQL Browser discovery | Underlying DNS/network/timeout category | The connection to the host {0}, named instance {1} failed. Error: "{2}". Verify the server and instance names and check that no firewall is blocking UDP traffic to port 1434. For SQL Server 2005 or later, verify that the SQL Server Browser Service is running on the host. | DNS, UDP socket, send and receive failures share this wrapper. `{2}` contains exception type/text via `toString()`, not a preserved cause. | [Browser](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java#L9130-L9227) |
| `R_notConfiguredToListentcpip` | SQL Browser discovery | `configuration` | The server {0} is not configured to listen with TCP/IP. | Browser response lacks `tcp;`; placeholder is supplied as instance name. | [Trigger](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java#L9215-L9227) |
| `R_notSQLServer` | Prelogin | `protocol_error` / `network_connectivity` | The driver received an unexpected pre-login response. Verify the connection properties and check that an instance of SQL Server is running on the host and accepting TCP/IP connections at the port. This driver can be used only with SQL Server 2005 or later. | Invalid response → protocol; raw prelogin EOF → network. Both may use this text inside TCP wrapper. Historical wording is not a current support-lifecycle promise. | [Prelogin](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java#L4690-L4780) |
| `R_unsupportedServerVersion` | Prelogin | `configuration` | SQL Server version {0} is not supported by this driver. | Reported version fails compatibility check. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L55) |
| `R_noServerResponse` | Login / transport | `network_connectivity` | SQL Server did not return a response. The connection has been closed. | Packet-reader EOF with no header bytes/prior packets; not a timeout. | [Reader](../src/main/java/com/microsoft/sqlserver/jdbc/IOBuffer.java#L7100-L7190) |
| `R_truncatedServerResponse` | Login / transport | `network_connectivity` | SQL Server returned an incomplete response. The connection has been closed. | EOF during packet processing; can be nested under TLS. | [Reader](../src/main/java/com/microsoft/sqlserver/jdbc/IOBuffer.java#L7100-L7190) |
| `R_sslFailed` | TLS | `tls_security` or underlying cause | "encrypt" property is set to "{0}" and "trustServerCertificate" property is set to "{1}" but the driver could not establish a secure connection to SQL Server by using Secure Sockets Layer (SSL) encryption: Error: {2}. | Inspect retained cause: local file/class failures → configuration; explicit expiration → timeout; certificate rejection → TLS. | [Wrapper](../src/main/java/com/microsoft/sqlserver/jdbc/IOBuffer.java#L1975-L2034) |
| `R_sslRequiredNoServerSupport` | TLS negotiation | `tls_security` | The driver could not establish a secure connection to SQL Server by using Secure Sockets Layer (SSL) encryption. The application requested encryption but the server is not configured to support SSL. | Requested encryption incompatible with negotiated response. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L175) |
| `R_sslRequiredByServer` | TLS negotiation | `tls_security` | SQL Server login requires an encrypted connection that uses Secure Sockets Layer (SSL). | Not a rule that `encrypt=false` always fails. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L176) |
| `R_certNameFailed` | TLS certificate | `tls_security` | Failed to validate the server name "{0}"in a certificate during Secure Sockets Layer (SSL) initialization. Name in certificate "{1}" | Hostname mismatch; source spelling retained. `{1}` is not necessarily the CN. | [Validation](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerCertificateUtils.java#L149-L274) |
| `R_serverCertExpired` / `R_serverCertNotYetValid` | TLS certificate | `tls_security` | Server Certificate has expired: {0}: {1}<br>Server Certificate is not yet valid: {0}: {1} | Separate validity templates in file-based trust-manager path; other paths may use JDK wording. | [Resources](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L573-L574) |
| `R_serverCertError` | TLS certificate | `tls_security` or underlying configuration | Error validating Server Certificate: {0}: <br>{1}:<br>{2}. | `<br>` denotes actual newline. Broad wrapper can contain certificate details; sanitize diagnostics. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L575) |
| PKIX / trust anchor / revocation / cipher / protocol / peer-certificate rejection | TLS certificate / algorithms | `tls_security` | JDK/security-provider-dependent messages. | Generic handshake failure cannot prove a specific cause. Revocation checks depend on configuration. | [Trust managers](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerTrustManager.java), [SSL](../src/main/java/com/microsoft/sqlserver/jdbc/IOBuffer.java#L1750-L2034) |
| `R_ALPNFailed` | TLS / strict mode | `tls_security` | Failed to negotiate Application-Layer Protocol {0}. Server returned: {1}. | ALPN mismatch; retain strict/TDS8 context. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L577) |
| `R_illegalArgumentTrustManager` | Trust-manager setup | `configuration` / `unknown` | Internal error. Peer certificate chain or key exchange algorithm can not be null or empty. | Invalid input, not proof of untrusted CA or wrong password. | [Guard](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerTrustManager.java#L134-L141) |
| Store path / password / type failure | TLS trust-store setup | `configuration` | Local file/security-provider message, often under `R_sslFailed`. | Distinguish unreadable/corrupt local store from peer trust rejection; do not recommend disabling verification. | [Trust setup](../src/main/java/com/microsoft/sqlserver/jdbc/IOBuffer.java#L1750-L1868) |
| `R_readCertError` | TLS / authentication certificate setup | `configuration` | Error reading certificate, please verify the location of the certificate. | Local read/setup failure, not server rejection. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L548) |
| `R_pvkParseError` / `R_pvkHeaderError` | TLS / authentication key setup | `configuration` | Could not read Private Key from PVK, check the password provided.<br>Cannot parse the PVK, PVK file does not contain the correct header. | Separate local key parse/format errors. | [Resources](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L546-L547) |
| `R_SetAuthenticationWhenIntegratedSecurityTrue` | Authentication configuration | `configuration` | Cannot set "Authentication" with "IntegratedSecurity" set to "true". | Conflicting options. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L376) |
| `R_NtlmNoUserPasswordDomain` | Authentication configuration | `configuration` | "User" (or "UserName") and "Password" connection properties must be specified for NTLM authentication. | Message does not require a domain despite resource name. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L377) |
| `R_SetAccesstokenWhenIntegratedSecurityTrue` | Authentication configuration | `configuration` | Cannot set the AccessToken property if the "IntegratedSecurity" connection string keyword has been set to "true". | Token/integrated-auth conflict. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L378) |
| `R_IntegratedAuthenticationWithUserPassword` | Authentication configuration | `configuration` | Cannot use "Authentication=ActiveDirectoryIntegrated" with "User", "UserName" or "Password" connection string keywords. | Incompatible credential inputs. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L379) |
| `R_ManagedIdentityAuthenticationWithPassword` | Authentication configuration | `configuration` | Cannot use "Authentication={0}" with "Password" connection string keyword. | Password incompatible with selected mode. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L380) |
| `R_AccessTokenWithUserPassword` | Authentication configuration | `configuration` | Cannot set the AccessToken property if "User", "UserName" or "Password" has been specified in the connection string. | Token/password conflict. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L381) |
| `R_AccessTokenCannotBeEmpty` | Authentication configuration | `configuration` | AccessToken cannot be empty. | Empty provided token, not rejected token. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L383) |
| `R_SetBothAuthenticationAndAccessToken` | Authentication configuration | `configuration` | Cannot set the AccessToken property if "Authentication" has been specified in the connection string. | Conflicting authentication inputs. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L384) |
| `R_NoUserPasswordForActivePassword` | Authentication configuration | `configuration` | Both "User" (or "UserName") and "Password" connection string keywords must be specified, if "Authentication=ActiveDirectoryPassword". | Missing required inputs. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L385) |
| `R_NoUserPasswordForActiveServicePrincipal` | Authentication configuration | `configuration` | Both "UserName" and "Password" connection string keywords must be specified, if "Authentication=ActiveDirectoryServicePrincipal". | Missing principal credentials. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L386) |
| `R_NoUserOrCertForActiveServicePrincipalCertificate` | Authentication configuration | `configuration` | "Both "UserName" and "clientCertificate" connection string keyword must be specified, if "Authentication=ActiveDirectoryServicePrincipalCertificate". | Missing certificate/principal; leading quote is in source. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L387) |
| `R_NoUserPasswordForSqlPassword` | Authentication configuration | `configuration` | Both "User" (or "UserName") and "Password" connection string keywords must be specified, if "Authentication=SqlPassword". | Missing SQL credentials. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L388) |
| `R_BothUserPasswordandDeprecated` | Authentication configuration | `configuration` | Both "User" (or "UserName"), "Password" and "AADSecurePrincipalId", "AADSecurePrincipalSecret" connection string keywords are specified, please use "User" (or "UserName"), "Password" only. | Current/deprecated credential inputs conflict. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L389) |
| `R_notConfiguredForIntegrated` | Native auth setup | `configuration` | This driver is not configured for integrated authentication.<br>Unable to load authentication DLL {0} | Separate outer/nested templates. Original loader exception may be replaced with synthetic `UnsatisfiedLinkError`. | [JNI](../src/main/java/com/microsoft/sqlserver/jdbc/AuthenticationJNI.java#L52-L118) |
| `R_UnableLoadMSSQLAuthDll` | Native Entra setup | `configuration` | Unable to load mssql-auth.dll. Error code: 0x{0}. For details, see: http://go.microsoft.com/fwlink/?LinkID=513072 | Retain native error code; do not guess cause. | [Native path](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java#L7090-L7182) |
| `R_integratedAuthenticationFailed` | Integrated authentication | `authentication` or underlying cause | Integrated authentication failed. | Broad JNI/GSS/JAAS mechanism failure. | [Kerberos](../src/main/java/com/microsoft/sqlserver/jdbc/KerbAuthentication.java#L131-L194) |
| `R_kerberosLoginFailed` / `R_kerberosLoginFailedForUsername` | Kerberos | `authentication` or underlying configuration | Kerberos Login failed: {0} due to {1} ({2})<br>Cannot login with Kerberos principal {0}, check your credentials. {1} | Separate composable templates. JAAS wrapper can set vendor 18456 without a server error. Ticket/SPN/delegation cause needs evidence. | [Wrapper](../src/main/java/com/microsoft/sqlserver/jdbc/KerbAuthentication.java#L131-L152) |
| `R_unsafeJaasLoginConfigProperty` | JAAS configuration | `configuration` | The system property "java.security.auth.login.config" must be a local file path or file: URI for Kerberos authentication. Non-local URLs (e.g. http, https, ldap, jar, rmi) are not permitted. Set useDefaultJaasConfig=true to bypass the JVM-wide JAAS configuration. | Local security configuration restriction, not SQL network policy. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L268-L272) |
| `R_UnableToFindClass` / `R_ibmModuleNotFound` / `R_moduleNotFound` | Auth dependency | `configuration` | Unable to locate specified class: {0}<br>com.ibm.security.auth.module.Krb5LoginModule module was not found.<br>Neither com.sun.security.auth.module.Krb5LoginModule nor com.ibm.security.auth.module.Krb5LoginModule was found. | Separate class/module errors. | [Resources](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L587-L589) |
| `R_ntlmHmacMD5Error` | NTLM setup | `configuration` | Unable to initialize NTLM authentication: HMAC-MD5 initialization error. | Local mechanism/cryptographic initialization. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L515) |
| `R_ntlmSignatureError` / `R_ntlmMessageTypeError` | NTLM challenge | `authentication` | NTLM Challenge Message signature error: {0}<br>NTLM Challenge Message type error: {0} | Separate challenge-validation errors, not necessarily wrong credentials. | [Resources](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L516-L517) |
| `R_ntlmAuthenticateError` | NTLM response | `authentication` | NTLM error when constructing Authenticate Message: {0} | Response construction failure. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L518) |
| `R_ntlmNoTargetInfo` / `R_ntlmUnknownValue` | NTLM challenge | `authentication` | NTLM Challenge Message is missing TargetInfo.<br>NTLM Challenge Message TargetInfo error: unknown value "{0}" | Separate challenge structure errors. | [Resources](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L519-L520) |
| `R_MSALMissing` | Auth dependency | `configuration` | Failed to load MSAL4J Java library for performing {0} authentication. | Missing/incompatible library. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L511) |
| `R_DLLandMSALMissing` | Auth dependency | `configuration` | Failed to load both {0} and MSAL4J Java library for performing {1} authentication. Please install one of them to proceed. | Neither supported provider available in the selected path. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L512) |
| `R_MSALExecution` | Token acquisition | `authentication` or underlying cause | Failed to authenticate the user {0} in Active Directory (Authentication={1}). | Prefix followed by provider detail. Correction may discard/replace causes; no AADSTS classifier or privacy redaction. | [Correction](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerMSAL4JUtils.java#L565-L593) |
| `R_ADALAuthenticationMiddleErrorMessage` | Native token acquisition | `authentication` | Error code 0x{0}; state {1}. | Legacy-named native error resource; numeric state is not JDBC SQLState. | [Native path](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java#L7090-L7182) |
| `R_ManagedIdentityTokenAcquisitionError` | MI / default credential | `authentication` or underlying cause | Failed to acquire managed identity token. The request for the token did not complete successfully. See the inner exception for details. | Also used by DefaultAzureCredential. Retained runtime cause can establish timeout/network/configuration. | [Acquisition](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerSecurityUtility.java#L346-L501) |
| `R_ManagedIdentityTokenAcquisitionFail` | MI / default credential | `authentication` | Failed to acquire managed identity token. Request for the token succeeded, but no token was returned. The token is null. | Empty result; not server rejection and not exclusive to MI. | [Acquisition](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerSecurityUtility.java#L346-L501) |
| Provider timeout, user cancellation, rejected/expired secret or certificate, refresh failure, UI required | Token acquisition | `timeout` / `canceled` / `authentication` | Provider-dependent messages/codes; no distinct fixed JDBC template per cause. | Require positive cause/code evidence. MSAL may transform timeout causes. Device-code flow is external callback/token-provider behavior, not a built-in driver mode. | [MSAL](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerMSAL4JUtils.java), [modes](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerDriver.java#L75-L133) |
| `R_InvalidAccessTokenCallbackClass` | Token callback | Underlying cause category | Invalid accessTokenCallbackClass: {0} | Wraps construction and invocation. A valid class throwing a token timeout can produce this message. | [Callback](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java#L6968-L7006) |
| `R_invalidClassNameForProperty` / `R_unassignableError` | Custom socket/trust/callback setup | `configuration` | The value specified by the {0} property is not a valid Java class name: {1}.<br>The class specified by the {0} property must be assignable to {1}. | Separate construction/type errors; inspect retained cause. | [Construction](../src/main/java/com/microsoft/sqlserver/jdbc/Util.java#L1118-L1146) |
| Server 18456 | Login | `authentication` | Exact text supplied by SQL Server, not a JDBC resource. | Login rejected, not always wrong password. Confirm server-error object; local Kerberos can also set 18456. SQLState is not universally `28000`. | [State mapping](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerException.java#L496-L547) |
| Server 18486 / 18488 | Login | `authentication` | Server-supplied exact text. | Account lockout / password-change-required family. Driver names 18488 `PASSWORD_EXPIRED`; preserve actual server wording/state. | [Constants](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerException.java#L68-L71) |
| Server 18452 / 18470 / 18487 | Login | Candidate `authentication` | Server-supplied; no dedicated driver handling/template found. | Original inventory associates untrusted domain / disabled login / password expiry. Confirm subtype from actual service catalog/response. | [Server parsing](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerError.java#L249-L269) |
| Server 40615; firewall, public-access, private-endpoint, VNet, ACL, tenant-policy denial | Login / policy | `access_policy` with explicit evidence | Server/provider-dependent; no per-policy JDBC templates. | Require explicit denial. TCP timeout/DNS failure alone is insufficient. 40615 mapping requires service catalog/response confirmation. | [Server parsing](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerError.java#L249-L269) |
| Server 4060 | Login / database open | `unknown` until clarified | Server-supplied. Source comment: Cannot open database "%.*ls" requested by the login. The login failed. | Wrong database, access failure, or availability; transient-list membership does not prove transient cause. | [Transient list](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerError.java#L25-L130) |
| Server 40197 / 40143 / 40166 / 40540 / 40020 | Login / availability | `server_availability` | Server-supplied exact text. | Service failure family; precise maintenance/failover cause is not guaranteed. | [Transient list](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerError.java#L25-L130) |
| Server 40501 / 40613 | Login / availability | `server_availability` | Server-supplied exact text. | Service busy / database unavailable; not uniquely serverless resume. | [Transient list](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerError.java#L25-L130) |
| Server 10928 / 10929 / 49918 / 49919 / 49920 | Login / resource limits | `server_availability` | Server-supplied exact text. | Resource/operation pressure, not necessarily connection-count exhaustion. | [Transient list](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerError.java#L25-L130) |
| Server 4221 / 42108 / 42109 | Login / replica or SQL pool | `server_availability` | Server-supplied exact text. | Secondary transition wait / SQL pool paused / SQL pool warming. Pool comments do not establish generic SQL Database serverless behavior. | [Transient list](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerError.java#L25-L130) |
| Server 10053 / 10054 / 64 | Login / transport | `network_connectivity` | Server/provider-supplied exact text. | Server numbers are not automatically assigned to Java socket exceptions. | [Transient list](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerError.java#L80-L105) |
| Server 233 | Login initialization | `unknown` until clarified | Server-supplied exact text. | Source lists transport, version, load, and resource possibilities. | [Transient list](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerError.java#L90-L100) |
| `R_invalidTDS` / `R_unexpectedToken` | Prelogin / login protocol | `protocol_error` | The TDS protocol stream is not valid.<br> Unexpected token {0}. | Second template starts with a space and is appended by helper. | [Helpers](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java#L4982-L4993) |
| `R_FedAuthRequiredPreLoginResponseInvalidValue` | FedAuth prelogin | `protocol_error` | Server sent an unexpected value for FedAuthRequired PreLogin Option. Value was {0}. | Invalid option value, not rejected credentials. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L362) |
| `R_FedAuthInfoLengthTooShortForCountOfInfoIds` | FedAuth metadata | `protocol_error` | The FedAuthInfo token must at least contain 4 bytes indicating the number of info IDs. | Invalid server metadata. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L363) |
| `R_FedAuthInfoInvalidOffset` | FedAuth metadata | `protocol_error` | FedAuthInfoDataOffset points to an invalid location. Current dataOffset is {0}. | Invalid offset. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L364) |
| `R_FedAuthInfoFailedToReadData` | FedAuth metadata | `protocol_error` or underlying I/O | Failed to read FedAuthInfoData. | Retain underlying read evidence if present. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L365) |
| `R_FedAuthInfoLengthTooShortForData` | FedAuth metadata | `protocol_error` | FEDAUTHINFO token stream is not long enough ({0}) to contain the data it claims to. | Length inconsistency. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L366) |
| `R_FedAuthInfoDoesNotContainStsurlAndSpn` | FedAuth metadata | `protocol_error` | FEDAUTHINFO token stream does not contain both STSURL and SPN. | Required metadata missing. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L367) |
| `R_UnrequestedFeatureAckReceived` / `R_UnknownFeatureAck` | Feature negotiation | `protocol_error` | Unrequested feature acknowledge is received. Feature ID: {0}.<br>Unknown feature acknowledge is received. | Separate unrequested/unknown responses. | [Resources](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L370-L375) |
| `R_FedAuthFeatureAckContainsExtraData` / `R_FedAuthFeatureAckUnknownLibraryType` | FedAuth negotiation | `protocol_error` | Federated authentication feature extension ack for ADAL and Security Token includes extra data.<br>Attempting to use unknown federated authentication library. Library ID: {0}. | Separate protocol errors; legacy ADAL wording is not a Java dependency error. | [Resources](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L371-L372) |
| `R_InvalidAEVersionNumber` / `R_InvalidVectorVersionNumber` / `R_InvalidJSONVersionNumber` | Feature negotiation | `protocol_error` | Received invalid version number "{0}" for Always Encrypted.<br>Received invalid version number "{0}" for vector feature negotiation.<br>Received invalid version number "{0}" for JSON feature negotiation. | Separate invalid negotiated-version templates. | [Resources](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L319-L322) |
| `R_unknownUTF8SupportValue` / `R_unknownAzureSQLDNSCachingValue` | Feature negotiation | `protocol_error` | Unknown value for UTF8 support.<br>Unknown value for Azure SQL DNS Caching. | Separate invalid feature-value templates. | [Resources](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L502-L503) |
| `R_unknownVectorSupportValue` / `R_unknownJSONSupportValue` | Feature negotiation | `protocol_error` | Unexpected version value received for vector support feature negotiation.<br>Unexpected version value received for JSON support feature negotiation. | Separate version-value errors. | [Resources](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L504-L505) |
| `R_enhancedRoutingFeatureAckContainsExtraData` | Routing negotiation | `protocol_error` | Enhanced routing feature extension ack should contain exactly 1 byte of data. | Invalid ACK length. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L373) |
| `R_invalidEnhancedRoutingInfo` / `R_invalidRoutingInfo` | Routing | `protocol_error` | Invalid enhanced routing information received.<br>Unexpected routing information received. Please check your connection properties and SQL Server configuration. | Separate metadata/control-flow errors; latter also covers routing combined with mirroring. | [Routing](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java#L3980-L4126) |
| `R_multipleRedirections` | Routing | `protocol_error` | Too many redirections have occurred. Only {0} redirections per login is allowed. | Limit is 10; exceeded count does not prove a graph cycle. | [Routing](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java#L3980-L4080) |
| `R_timedOutBeforeRouting` | Routing budget | `timeout` | The timeout expired before connecting to the routing destination. | Wrapped in TCP failure; internal UNSUPPORTED_CONFIG at this site despite timeout meaning. | [Trigger](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java#L4100-L4116) |
| Redirect target DNS/TCP/TLS/login failure | Redirected connection | Underlying category | Underlying message; host may show `{0} (redirected from {1})`. | Retain `redirected=true`; do not replace specific underlying cause with routing category. | [Routing](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java#L3980-L4126) |
| `R_nullConnection` / `R_invalidConnection` | URL validation | `configuration` | The connection URL is null.<br>The connection URL is invalid. | Different API paths. Direct `Driver.connect()` can return null for unrecognized prefix. | [Driver](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerDriver.java#L1445-L1543) |
| `R_errorConnectionString` / `R_errorServerName` | URL validation | `configuration` | The connection string contains a badly formed name or value.<br>The serverName connection property value {0} is badly formed. | Local parsing, not DNS. Missing server name can default to localhost. | [Resources](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L118-L119), [default](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java#L2775-L2792) |
| `R_invalidPortNumber` | Property validation | `configuration` | The port number {0} is not valid. | Invalid input/range, not port unreachable. | [Resource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L72) |
| `R_invalidBooleanValue` / `R_propertyMaximumExceedsChars` | Property validation | `configuration` | The property {0} does not contain a valid boolean value. Only true or false can be used.<br>The {0} property exceeds the maximum number of {1} characters. | Separate value/length checks. | [Resources](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L70-L71) |
| `R_invalidTimeOut` / `R_invalidSocketTimeout` | Timeout configuration | `configuration` | The timeout {0} is not valid.<br>The socketTimeout {0} is not valid. | Invalid input, not expired deadline. | [Resources](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java) |
| `R_invalidQueryTimeout` / `R_invalidCancelQueryTimeout` | Connection timeout settings | `configuration` | The queryTimeout {0} is not valid.<br>The cancel timeout value {0} is not valid. | Settings can fail connection initialization; actual query timeout/cancel events are not connection-open errors. | [Resources](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java) |
| `R_invalidConnectRetryCount` / `R_invalidConnectRetryInterval` | Retry configuration | `configuration` | Connection retry count {0} is not valid.<br>Connection retry interval {0} is not valid. | Invalid values, not exhausted recovery. | [Resources](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L560-L563) |
| `R_invalidPacketSize` / `R_packetSizeTooBigForSSL` | Packet configuration | `configuration` | The packetSize {0} is not valid.<br>SSL encryption cannot be used with a network packet size larger than {0} bytes.  Please check your connection properties and SQL Server configuration. | Separate size/SSL compatibility errors; two spaces after `bytes.` in SSL template. | [Resources](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L76-L77) |
| `R_invalidAuthenticationScheme` / `R_InvalidConnectionSetting` | Authentication / enum configuration | `configuration` | The authenticationScheme {0} is not valid.<br>The {0} value "{1}" is not valid. | Property determines meaning. Unknown property names may be ignored instead of throwing. | [Resources](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java), [normalization](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerDriver.java#L1360-L1439) |
| `R_invalidSSLProtocol` / `R_invalidFipsConfig` | TLS configuration | `configuration` | SSL Protocol {0} label is not valid. Only TLS, TLSv1, TLSv1.1, and TLSv1.2 are supported.<br>Unable to verify FIPS mode settings. | Invalid setup, distinct from failed negotiation with valid properties. | [Resources](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java) |
| `R_invalidpropertyValue` / `R_InvalidIPAddressPreference` | Property validation | `configuration` | The data type of connection property {0} is not valid. All the properties for this connection must be of String type.<br>IP address preference {0} is not valid. | Separate checks; supported property-only objects are exceptions to general string-input rule. | [Resources](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java) |
| `R_invalidapplicationIntent` / `R_invalidresponseBuffering` / `R_invalidselectMethod` | Property validation | `configuration` | The applicationIntent connection property {0} is not valid.<br>The responseBuffering connection property {0} is not valid.<br>The selectMethod {0} is not valid. | Separate connection option validations. | [Resources](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L161-L166) |
| `R_failoverPartnerWithoutDB` / `R_invalidPartnerConfiguration` | Mirroring setup | `configuration` | databaseName is required when using the failoverPartner connection property.<br>The database {0} on server {1} is not configured for database mirroring. | Missing required database or invalid partner configuration. | [Resources](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L158-L159) |
| `R_dbMirroringWithMultiSubnetFailover` / `R_dbMirroringWithReadOnlyIntent` | Mirroring compatibility | `configuration` | Connecting to a mirrored SQL Server instance using the multiSubnetFailover connection property is not supported.<br>Connecting to a mirrored SQL Server instance using the ApplicationIntent ReadOnly connection property is not supported. | Unsupported combinations. | [Resources](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L45-L46) |
| `R_ipAddressLimitWithMultiSubnetFailover` | Multi-subnet setup | `configuration` | Connecting with the multiSubnetFailover connection property to a SQL Server instance configured with more than {0} IP addresses is not supported. | Limit 64. TNIR can instead disable itself and use full timeout; strategy matters. | [SocketFinder](../src/main/java/com/microsoft/sqlserver/jdbc/IOBuffer.java#L2589-L2678) |
| `R_InvalidRuleFormat` | Retry-rule setup | `configuration` | Wrong number of parameters supplied to rule. Number of parameters: {0}, expected: 2 or 3. | Parsing/validation error, not server failure. | [Parser](../src/main/java/com/microsoft/sqlserver/jdbc/ConfigurableRetryRule.java) |
| `R_keyStoreAuthenticationNotSet` | AE provider setup during connection open | `configuration` | "keyStoreAuthentication" connection string keyword must be specified, if "{0}" is specified. | Connection initialization validation; later key-decryption failures are not login errors. | [Resources](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java), [setup](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java) |
| `R_keyStoreSecretOrLocationNotSet` / `R_keyStoreSecretNotSet` | AE provider setup during connection open | `configuration` | Both "keyStoreSecret" and "keyStoreLocation" must be set, if "keyStoreAuthentication=JavaKeyStorePassword" has been specified in the connection string.<br>"keyStoreSecret" must be set, if "keyStoreAuthentication=KeyVaultClientSecret" has been specified in the connection string. | Separate required-input validations. | [Resources](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java) |
| `R_keyVaultProviderClientKeyNotSet` / `R_keyVaultProviderNotSupportedWithKeyStoreAuthentication` | AE provider setup during connection open | `configuration` | "keyVaultProviderClientKey" must be set, if "keyVaultProviderClientId" has been specified in the connection string.<br>"keyStoreAuthentication" cannot be used with "keyVaultProviderClientId" or "keyVaultProviderClientKey" in the connection string. | Separate missing/conflicting inputs. | [Resources](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java) |
| `R_invalidKeyStoreFile` | AE provider setup during connection open | `configuration` | Cannot parse "{0}". Either the file format is not valid or the password is not correct. | Include when provider initializes during open; not a universal TLS trust-store error. | [Provider](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerColumnEncryptionJavaKeyStoreProvider.java#L239) |
| `R_enclavePropertiesError` / `R_enclaveInvalidAttestationProtocol` | Enclave connection configuration | `configuration` | Exact resource text, including documentation URLs, is available at the linked source. | Invalid enclave settings during open; attestation errors during query execution are not login errors. | [Resources](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java#L528-L529) |
| JVM heap / native memory / direct-buffer exhaustion | Client memory allocation during connection open / recovery | `client_resource_exhaustion` | JVM-dependent `OutOfMemoryError` text; no general JDBC connection-memory resource or guaranteed SQLException wrapper. | Insufficient memory in the client process. Does not establish a driver memory leak: application load and other libraries share the process. Assign only on explicit allocation-failure evidence; telemetry itself may fail under memory pressure. Do not blindly catch/retry JVM errors. | [Connection allocation paths](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java), [I/O buffers](../src/main/java/com/microsoft/sqlserver/jdbc/IOBuffer.java) |
| Native thread / process-thread limit exhausted | Client thread allocation during connection open / recovery | `client_resource_exhaustion` | JVM/OS-dependent error, potentially `OutOfMemoryError` with thread-creation detail; no dedicated JDBC resource. | Thread exhaustion differs from Java heap exhaustion. Require explicit thread-allocation evidence; a generic timeout does not prove resource starvation. | [SocketFinder](../src/main/java/com/microsoft/sqlserver/jdbc/IOBuffer.java#L2618-L2738), [shared timer](../src/main/java/com/microsoft/sqlserver/jdbc/SharedTimer.java) |
| File-descriptor / socket-handle / ephemeral-port exhaustion | Client socket allocation / TCP connect | `client_resource_exhaustion` | OS/JDK-dependent socket or I/O exception; may be embedded in `R_tcpipConnectionFailed`. | Assign only when local resource exhaustion is confirmed. `BindException`, `Cannot assign requested address`, refusal, or reset alone can have other causes; otherwise retain configuration/network/unknown classification. No dedicated driver capacity counter or error code is exposed here. | [Socket creation](../src/main/java/com/microsoft/sqlserver/jdbc/IOBuffer.java#L2618-L2738), [TCP wrapper](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerException.java#L437-L449) |
| Pool at capacity with no available connection | Application connection-pool acquisition | `client_resource_exhaustion` | Pool-library-specific acquisition error/timeout; not a Microsoft JDBC driver message. | Use when the owning pool confirms capacity is exhausted. SQLServerPooledConnection supplies a physical connection and logical handles, not a bounded borrow queue with a maximum pool size. Pool wait timeout alone may instead reflect failed physical connection creation; preserve that underlying cause. Record origin as external pool and the wait timeout separately. | [Driver pool primitive, not external-pool implementation](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPooledConnection.java#L100-L162) |
| Executor rejection | Client timer / executor use | `client_resource_exhaustion` only with capacity evidence; otherwise `connection_lifecycle` or `internal_error` | JDK/executor-dependent `RejectedExecutionException` text; no dedicated JDBC rejection template. | Rejection does not universally mean overload: a shutdown executor also rejects work. Classify confirmed saturation as resource exhaustion, shutdown/lifecycle separately, unexpected scheduler-state violations as internal error. | [Scheduling](../src/main/java/com/microsoft/sqlserver/jdbc/SharedTimer.java) |
| SharedTimer reference-count / scheduling invariant failure | Client shared-timer lifecycle | `internal_error` | removeRef() called more than actual references<br>Cannot schedule tasks after shutdown | Two exact hardcoded `IllegalStateException` messages, not resource strings. These indicate an invalid timer lifecycle/state, not evidence of insufficient memory, pool capacity, or server availability. Include only when surfaced in the connection lifecycle being classified. | [SharedTimer](../src/main/java/com/microsoft/sqlserver/jdbc/SharedTimer.java) |
| JDK/driver incompatibility / linkage or dependency failure | Runtime initialization | `configuration` | JVM-dependent message, potentially before any JDBC exception exists. | Class/version/dependency incompatibility remains configuration; confirmed memory/thread exhaustion belongs to client_resource_exhaustion instead. Do not treat every JVM Error as a recoverable connection failure. | [Driver](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerDriver.java), [dependency loading](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerSecurityUtility.java) |
| `R_crClientAllRecoveryAttemptsFailed` | Recovery | `connection_recovery` | The connection is broken and recovery is not possible. The client driver attempted to recover the connection one or more times and all attempts failed. Increase the value of ConnectRetryCount to increase the number of recovery attempts. | Exhausted attempts; inspect last cause before increasing retries. | [Trigger](../src/main/java/com/microsoft/sqlserver/jdbc/IdleConnectionResiliency.java#L528-L531) |
| `R_crClientNoRecoveryAckFromLogin` | Recovery | `connection_recovery` | The server did not acknowledge a recovery attempt, connection recovery is not possible. | Missing ACK under recovery/routing guards; internal INVALID_TDS. | [Guard](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java#L8040-L8049) |
| `R_crServerSessionStateNotRecoverable` | Recovery | `connection_recovery` | The connection is broken and recovery is not possible. The connection is marked by the server as unrecoverable. No attempt was made to restore the connection. | Also wraps an already-attempted reconnect logon failure; “No attempt” is not universally true. | [Wrapper](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java#L4509-L4515), [precheck](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java#L5088-L5092) |
| `R_crClientUnrecoverable` | Recovery | `connection_recovery` | The connection is broken and recovery is not possible. The connection is marked by the client driver as unrecoverable. No attempt was made to restore the connection. | Client veto, including unprocessed responses. Recovery precedes a new command, not arbitrary replay of failed work. | [Guard](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java#L5083-L5087) |
| `R_crClientSSLStateNotRecoverable` | Recovery | `connection_recovery` | The server did not preserve SSL encryption during a recovery attempt, connection recovery is not possible. | Encryption-level equality check, not only downgrade detection. | [Guard](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java#L4497-L4507) |
| `R_connectionIsClosed` | Closed connection / pooled handle | `connection_lifecycle` | The connection is closed. | Usually closed/invalidated handle; does not explain original close. Also used by timeout task termination branch. | [Proxy](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnectionPoolProxy.java#L72-L77), [timer](../src/main/java/com/microsoft/sqlserver/jdbc/TDSTimeoutTask.java#L38-L59) |
| `R_physicalConnectionIsClosed` | Pooling | `connection_lifecycle` | The physical connection is closed for this pooled connection. | Pool physical-connection field is null; not an active network-health check. | [Guard](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPooledConnection.java#L115-L120) |
| `R_failedToCreateXAConnection` | XA control-connection open | Underlying connection category | Failed to create the XA control connection. Error: "{0}" | Classify underlying open failure. XA enlist/commit/rollback operation errors are outside login scope. | [XA connection](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerXAConnection.java) |
| Explicit interruption/cancellation | Connection / token interruption | `canceled` | Provider/JVM-dependent; no universal connection-canceled resource. | Require actual cancellation evidence. Socket closure alone is insufficient; no universal interruptible DNS/open contract. | [MSAL interruption](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerMSAL4JUtils.java#L140-L148) |
| Budget exhausted; last error rethrown | Login retry budget | Last specific category or `timeout` | May retain last network/server message rather than dedicated timeout text. | Record budget exhaustion separately when known. A retry-list match does not make statement or COMMIT replay safe. | [Outer retries](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java#L2450-L2526), [inner retries](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java#L4144-L4195) |
| Unrecognized server/provider/runtime failure or insufficient evidence | Any connection phase | `unknown` | Preserve sanitized received message; no invented canonical template. | Separate SQLState/vendor number/server numeric state/internal driver code. `08S01` is broad; X/Open uses `08001`/`08006`; null/empty states occur. Optional `ClientConnectionId` is not a stable message key. Source audit at `693c9489`; not live fault-injection validation. | [Exceptions](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerException.java#L224-L591), [termination](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java#L5002-L5028) |

## OpenTelemetry connection span: MVP shipping contract

This section specifies the proposed **failed-connection observability MVP**: a connection-open span, meaningful phase children, and a structured error event. It enables grouping by error category and authentication method without verbose tracing. It does not include successful-connection latency reporting, statements, result sets, COMMIT/ROLLBACK, or SQL Server CPU/I/O metrics.

> **MVP scope: export failed connection opens only.** Export the connection-open span and its retained phase/attempt children only when the overall open ends in failure, timeout, or cancellation. Do not export successful connection opens or their children, including opens that succeed after retries. Successful phase children within an overall failed open remain included to explain progress before failure. The successful-connection worked example below is **for illustration only—not an MVP export payload**.

### 1. What one connection span contains

**At a glance — successful connection, for illustration only (not exported by the MVP).** This example uses token-based authentication and TDS 7.x to show all applicable phase types, including optional named-instance discovery and token acquisition. Times are illustrative.

```text
mssql.driver.connection.open                      CLIENT   UNSET  150 ms
├─ mssql.driver.connection.configuration          INTERNAL UNSET    2 ms
├─ mssql.driver.connection.attempt                INTERNAL UNSET  138 ms
│  ├─ mssql.driver.connection.instance_discovery   INTERNAL UNSET    5 ms  [if needed]
│  │  └─ mssql.driver.connection.dns              INTERNAL UNSET    3 ms  [Browser host lookup]
│  ├─ mssql.driver.connection.dns                 INTERNAL UNSET    3 ms  [if transport resolves again]
│  ├─ mssql.driver.connection.socket_connect      INTERNAL UNSET   12 ms
│  ├─ mssql.driver.connection.prelogin            INTERNAL UNSET    8 ms
│  ├─ mssql.driver.connection.tls                 INTERNAL UNSET   30 ms
│  └─ mssql.driver.connection.login               INTERNAL UNSET   80 ms
│     └─ mssql.driver.connection.token_acquisition INTERNAL UNSET   40 ms  [if driver acquires token]
└─ mssql.driver.connection.initialize             INTERNAL UNSET   10 ms
```

- Root attribute: `mssql.connection.outcome=success`. No error event or error attributes. Native `UNSET` means no explicit status was assigned; the outcome attribute confirms success.
- Only executed phases appear. SQL-password or pre-supplied-token authentication has no token-acquisition child; connections not using SQL Browser have no instance-discovery child. Resolver calls are represented where they actually occur, not duplicated for presentation.
- Parent durations **include** their children: the 40 ms token acquisition is part of the 80 ms login; the 3 ms Browser DNS lookup is part of the 5 ms discovery. Do not add these nested durations again. Configuration + attempt + initialization = 150 ms.
- Strict/TDS8 puts TLS before prelogin. Retries and redirects create additional attempt spans; their decisions are events. Pool reset and reconnect are separate operations, not extra phases of this successful physical open.
- **This tree is explanatory only.** The MVP exports the root and retained children only for an overall failed open; it does not export this successful example.

One open span describes one physical connection-open operation. Its attributes and events are separate maps; phase children are separate span records linked by native parent IDs, not embedded attributes or events.

```text
open span record
├─ native fields: name, kind, IDs, timestamps, status
├─ attributes: connection identity, settings, outcome
└─ events[]: name + timestamp + event attributes (no duration)
phase child record ── parent_span_id ──> open (or owning phase)
```

#### Native fields

| Native field | Value / meaning |
|---|---|
| `name` | `mssql.driver.connection.open`; constant, with no host, database, or user in the name. |
| `kind` | `CLIENT` for the open span; `INTERNAL` for phase children. Separately instrumented outgoing identity-service calls retain their own client-span semantics. |
| Trace ID / span ID / parent span ID | Native trace identity and parent relationship. Capture the caller's context at entry and propagate it across workers. These are not custom attributes or connection GUIDs. A root without a parent has no parent span ID. |
| Start / end timestamps | Actual operation boundaries, represented in OTLP as epoch nanoseconds. The difference describes elapsed span time; do not time only telemetry publication. |
| `status` | `ERROR` for a terminal failed open and its failing child; `UNSET` for successful phases. This is the native status, not the custom outcome attribute. No raw exception message in status description. |
| `events` | Timestamped records inside a span, including `mssql.driver.error`. Each event has its own name, timestamp, and key-value attributes; it is not a child span and has no duration. |

Field labels above are conceptual/decoded names; SDK and OTLP JSON field spellings may differ. Do not duplicate native trace identifiers as custom attributes.

Attributes are explicitly set: an error event's category is not automatically copied to its span, and parenting does not copy a parent's attributes to children. Use native `trace_id`, `span_id`, and `parent_span_id` to look up the owning root for connection GUID/user-agent and the owning attempt for its TDS UUID; these attributes are not implicitly inherited. The custom connection GUID is not a trace ID. The full maps are defined under **Metadata and full attribute schema** and **Events and failures** below.

#### Lifetime — implementation rules, not exported fields

| Rule | Required behavior |
|---|---|
| Start boundary | Start before driver-owned validation that can fail, once the request is recognized as belonging to this driver. |
| End boundary | End immediately before returning a usable physical connection or propagating terminal failure. Include internal retries, redirects, backoff, and required post-login initialization. |
| Operation scope | One overall physical-open operation, not one root per retry and not the connection's entire lifetime. External pool borrow/wait time is outside this driver span. |
| Clock handling | Use real timestamps and monotonic elapsed measurement. Numeric duration diagnostics use seconds. |
| Early validation failure | Emit the root and a configuration child when telemetry is available; omit unexecuted network phases. A URL belonging to another driver is not a failed SQL connection. |

### 2. Metadata and full attribute schema

Use the application-owned resource; do not rename the application to the JDBC driver or mutate its shared resource per connection. **Reuse the existing user-agent as one root attribute**, replacing separate driver-name/version, architecture, OS, and runtime span attributes. Children correlate through native trace identity and parent links; connection GUID lookup requires the owning root.

#### Application resource and instrumentation scope

| Key / field | Type | Value / rule |
|---|---|---|
| Instrumentation scope name | String | `com.microsoft.sqlserver.jdbc`; version is the actual driver version. |
| `service.name`, `service.version` | String | Application-supplied approved service identity, when available. Never derive from a host/user/database name. |

The native instrumentation-scope version remains the driver version: this is standard scope metadata, not a duplicate custom span attribute. Existing application-provided runtime/resource metadata need not be removed merely because it overlaps the user-agent; the driver does not add another copy.

#### User-agent fields and derived metadata

The current format has seven pipe-separated fields, built by [getUserAgent() and its field helpers](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java#L329-L400). Lengths below are character limits imposed by those helpers.

| Position | Field | Example | Replaces / enables downstream |
|---|---|---|---|
| 1 | User-agent format version | `1` | Version-aware payload parsing; distinct from telemetry schema version. |
| 2 | Driver product identifier | `MS-JDBC` | Driver-name identity; family `jdbc` can be derived from this known identifier. |
| 3 | Driver version, maximum 24 characters | `13.5.0.0-preview` | Separate `mssql.driver.version` span attribute. Actual format includes major, minor, patch, build, and release suffix. |
| 4 | Architecture, maximum 10 characters | `amd64` | Separate architecture attribute; raw Java `os.arch` spelling may require normalization. |
| 5 | OS family, maximum 10 characters | `Windows` | Separate OS-family attribute; normalize before mapping to standard OTel OS values. |
| 6 | OS details, maximum 44 characters | `Windows 11 10.0` | Combined OS name/version text. |
| 7 | JVM details, maximum 44 characters | `OpenJDK 64-Bit Server VM 21.0.4` | Combined VM name/version text, not guaranteed to be separable into exact standard runtime-name/version values. |

Example payload (plain pipe separators): `1|MS-JDBC|13.5.0.0-preview|amd64|Windows|Windows 11 10.0|OpenJDK 64-Bit Server VM 21.0.4`.

The driver removes characters outside letters, digits, spaces, `.`, `+`, `_`, and `-` from variable fields and truncates them. Missing fields become `Unknown`; construction failure falls back to `1|MS-JDBC|Unknown|Unknown|Unknown|Unknown|Unknown`. **Do not fabricate omitted/truncated details or split runtime text on spaces to infer exact name/version boundaries.** A collector/backend can parse the known format once without shipping duplicate attributes; OTel does not parse it automatically. Do not interpret unknown format versions using the version-1 layout. This bounded sanitization is not privacy redaction; approval rules are under **Export, privacy, and metrics**.

This payload does **not** replace application service identity, database authentication method, connection IDs, outcome, phase, error category, or telemetry schema version. Those are not included in the user-agent.

#### Full span attribute allowlist

`Required` means whenever telemetry for the operation can be recorded. Unknown conditional values are **omitted**, never the strings `null`, `N/A`, or a guessed value. The `mssql.*` keys below are proposed cross-driver attributes, not claims that OTel standardizes every key.

**Identity and placement**

| Attribute key | OTel type | Placement / requirement | Values and semantics |
|---|---|---|---|
| `mssql.connection.guid` | String | Root only; required | Random connection GUID allocated before DNS/TCP; stable across this open's attempts. Retain internally for the physical connection's lifetime to correlate later separately rooted reset/acquisition/recovery operations. Not a trace, user, or resource ID; never repeat on attempts, phases, or events. |
| `mssql.connection.id` | Int64 | Root only; optional diagnostic, not mandatory or part of the minimal MVP | Process-local driver connection ID, only when allocated and diagnostic enrichment is enabled. Not globally unique; omit from worked examples, children, and events. |
| `mssql.connection.client_connection_id` | String | Attempt only; conditional on allocation | Actual attempt's TDS UUID. Omit before allocation; never copy to root, phase children, or events. |
| `mssql.driver.user_agent.original` | String | Root only; when available and privacy-approved | Existing seven-field driver-generated `userAgentStr` described above. Replaces separate driver family/name/version, architecture, OS and runtime span attributes. Do not copy onto every child/event or use as a metric label. |
| `mssql.telemetry.schema.version` | String | Root; required | `1.0`; independently versioned from the driver. |
| `db.system.name` | String | Root/phase spans; required | `microsoft.sql_server` using the OTel database-system identifier. This deliberately normalizes the broader draft's shorthand `mssql`; pin the semantic-convention version when implementing. |
| `mssql.connection.origin` | String | Root; required | `driver`, `datasource`, `pooled_physical`, or `xa_control`; no invented external pool queue measurement. |
| `mssql.connection.endpoint_role` | String | Attempt/phase when known | `database`, `gateway`, `redirect_target`, `sql_browser`, `identity_provider`, `unknown`. Use `gateway` only when established by protocol/context. No endpoint value. |

**Authentication and settings**

Capture the selected safe connection settings on the **connection-open root**, after property merging, default resolution, and validation. Include effective defaults when known, not just explicitly supplied properties. Do not copy the full connection string or arbitrary property maps. Omit values not yet validated if configuration fails early. Children do not repeat this settings block; authentication context on auth-related children and explicitly defined effective attempt strategies are the limited exceptions in the table below.

Timeout and interval attributes use **seconds**, including conversion of JDBC `socketTimeout` from milliseconds. Configured encryption/authentication values describe intended settings, not proof that negotiation or authentication completed. The worked examples below include the full safe settings block using explicitly assumed effective values; these examples are not a statement that every driver version/environment has the same defaults.

| Attribute key | OTel type | Placement / requirement | Values and semantics |
|---|---|---|---|
| `mssql.authentication.method` | String | Root and auth-related children; required | `sql_password`, `integrated_native`, `integrated_kerberos`, `integrated_ntlm`, `entra_password`, `entra_integrated`, `managed_identity`, `service_principal_secret`, `service_principal_certificate`, `interactive`, `default_credential`, `access_token`, `access_token_callback`, `unknown`. Normalize validated database authentication, not exporter authentication. |
| `mssql.authentication.token_source` | String | Acquisition span/event when known | `msal`, `native`, `managed_identity`, `default_credential`, `callback`. Do not claim which member of a default chain succeeded unless observable. |
| `mssql.connection.encrypt` | String | Root; conditional | Validated effective `false`, `true`, or `strict`. Do not echo invalid raw input. |
| `mssql.connection.trust_server_certificate` | Boolean | Root; conditional | Effective validated setting; strict overrides are reflected. |
| `mssql.connection.application_intent` | String | Root; conditional | `read_write` or `read_only`. |
| `mssql.connection.multi_subnet_failover` | Boolean | Root; conditional | Validated setting. |
| `mssql.connection.transparent_network_ip_resolution` | Boolean | Root/attempt; conditional | Requested setting on root, effective strategy setting on attempt. |
| `mssql.connection.login_timeout` | Double | Root; conditional | Effective budget in seconds; does not claim an interruptible deadline for every provider or DNS call. |
| `mssql.connection.socket_timeout` | Double | Root; conditional | Configured seconds, converted from JDBC milliseconds; 0 means unlimited configured socket timeout, subject to connection-setup capping. |
| `mssql.connection.connect_retry_count` | Int64 | Root; conditional | Configured retry limit; not actual attempts. |
| `mssql.connection.connect_retry_interval` | Double | Root; conditional | Configured seconds between applicable retries. |
| `mssql.connection.transport_strategy` | String | Socket child; conditional | `serial`, `parallel`, `tnir`; no peer addresses. |
| `mssql.connection.tls_version` | String | TLS child; conditional | Actual negotiated version, e.g. `TLSv1.3`; omit if negotiation did not finish. |
| `mssql.connection.tds_version` | String | Attempt; conditional | Known protocol version only; distinguish configured strict mode from completed negotiation. |

**Outcome, attempts, and retries**

| Attribute key | OTel type | Placement / requirement | Values and semantics |
|---|---|---|---|
| `mssql.connection.outcome` | String | Root at end; required | `failure`, `timeout`, `canceled`; `success` only for an internally completed record excluded from MVP failure export. |
| `mssql.connection.failure_phase` | String | Terminal failed physical-open root only; never attempt or phase child | Actual failing phase: `configuration`, `instance_discovery`, `dns`, `socket_connect`, `prelogin`, `tls`, `token_acquisition`, `login`, `redirect`, `initialize`, or `unknown`. Preserve it before higher wrappers obscure it; the child span name identifies its phase. |
| `mssql.connection.attempt_count` | Int64 | Root at end; required | Total endpoint attempts started across all internal retry loops; 0 is valid for configuration failure. |
| `mssql.connection.retry_count` | Int64 | Root at end; required | Additional attempts actually started after failures; excludes redirects and parallel IP candidates. |
| `mssql.connection.redirect_count` | Int64 | Root at end; required | Redirects actually followed. |
| `mssql.connection.attempt` | Int64 | Attempt and descendants; required | One-based index across the open operation. |
| `mssql.connection.attempt_reason` | String | Attempt; required | `initial`, `retry`, `redirect`, `failover`. |
| `mssql.connection.attempt_outcome` | String | Attempt at end; required | `failure`, `timeout`, `canceled`, `success`, `redirect`. |
| `mssql.connection.budget_exhausted` | Boolean | Root; conditional | Actual budget accounting established exhaustion. A final specific error can remain more informative than a generic timeout label. |
| `mssql.error.category` | String | Terminal root/failing child; required | One of the 16 categories above, never an ambiguous table phrase such as “underlying category.” |
| `error.type` | String | Terminal root/failing child; required | Stable primary classification: `sqlserver.18456` for a confirmed server error; otherwise observed exception class or bounded failure identifier. Never raw message. |
| `mssql.telemetry.truncated` | Boolean | Root; required when per-open recording limits are exceeded | `true` indicates an incomplete diagnostic record. |
| `mssql.telemetry.dropped_span_count` | Int64 | Root; required when per-open recording limits are exceeded | Nonnegative count of spans omitted by the per-open limit. |
| `mssql.telemetry.dropped_event_count` | Int64 | Root; required when per-open recording limits are exceeded | Nonnegative count of events omitted by the per-open limit. |

**Category alignment:** this report uses `client_resource_exhaustion` to make the MVP's broader `resource_exhaustion` label explicitly client-side. Ship one canonical category, not both aliases. Server limits remain `server_availability`. Closed handles remain `connection_lifecycle`; confirmed internal invariants remain `internal_error`.

**Minimal phase and reachability reporting:** phase child names identify executed work; no separate `mssql.connection.phase` attribute is exported. Keep `mssql.error.phase` on the compact error event for standalone event queries. `mssql.connection.reachability` is deferred, not a required or exported MVP field; derive observed progress from retained children and events without inferring unobserved milestones or topology.

#### Attribute value reference — what each enum value means

These are **proposed telemetry string values**, not necessarily Java enum names. Use the exact spelling shown, not display labels or arbitrary user input. The allowlists above and below specify placement and required/optional status; this reference explains how to choose values. It does not make an optional field required or add a new attribute.

For optional fields, omit unavailable values. Use `unknown` only where explicitly listed, and do not emit the string `null`. Consumers must tolerate future values without misclassifying them. Boolean attributes use actual booleans, not strings; `mssql.connection.encrypt` is deliberately a string enum.

**Connection origin — which driver-owned entry path created the physical connection?**

| Attribute | Value | Meaning / selection rule |
|---|---|---|
| `mssql.connection.origin` | `driver` | Direct driver/DriverManager physical-open path, without a more specific known context. Does not mean “all JDBC activity.” |
| `mssql.connection.origin` | `datasource` | Ordinary DataSource physical open, not one explicitly identified as pooled or XA control creation. |
| `mssql.connection.origin` | `pooled_physical` | Creation of a physical connection explicitly on behalf of JDBC pooling. Not every logical borrow and not merely because an external pool somewhere uses a DataSource. |
| `mssql.connection.origin` | `xa_control` | The separate physical control connection used for XA coordination. Not every connection from an XADataSource. |

When paths overlap, choose the most specific **known** context: `xa_control` before `pooled_physical`, then `datasource`, then `driver`. Do not infer an external pool from the call stack. Origin is diagnostic entry-path information, not an error source or remote endpoint identity. It is a candidate for deferral if the final MVP does not need entry-path filtering; this reference itself does not change the current allowlist.

**Endpoint role — what is being contacted?**

| Attribute | Value | Meaning / selection rule |
|---|---|---|
| `mssql.connection.endpoint_role` | `database` | The logical database connection target; does not prove the remote process is a database engine rather than a gateway. |
| `mssql.connection.endpoint_role` | `gateway` | A gateway role positively established by protocol or trusted context. Never infer solely from Azure SQL use or a hostname. |
| `mssql.connection.endpoint_role` | `redirect_target` | Endpoint selected by a received routing instruction. Identifies a redirected attempt without exposing its address. |
| `mssql.connection.endpoint_role` | `sql_browser` | SQL Browser service contacted to resolve a named instance. |
| `mssql.connection.endpoint_role` | `identity_provider` | Authentication/token-service endpoint, not SQL Server. Use only for actual endpoint work, not every callback invocation. |
| `mssql.connection.endpoint_role` | `unknown` | Role could not be established; because the attribute is optional, omission is preferred when it adds no information. |

For a routed attempt, prefer `redirect_target` over the generic `database` role. Browser and token child names often already convey their role; repeating the attribute is useful only if the consumer needs it. Neither `origin` nor `endpoint_role` contains a host, IP address, database name, or URL.

**Authentication — database login mechanism versus token provider**

| Attribute | Value | Meaning |
|---|---|---|
| `mssql.authentication.method` | `sql_password` | SQL Server username/password authentication. No credential values are exported. |
| `mssql.authentication.method` | `integrated_native` | Native integrated authentication through the driver's native library/SSPI path. |
| `mssql.authentication.method` | `integrated_kerberos` | Java Kerberos/GSS integrated authentication. |
| `mssql.authentication.method` | `integrated_ntlm` | Driver NTLM integrated authentication. |
| `mssql.authentication.method` | `entra_password` | Entra username/password token-based authentication. |
| `mssql.authentication.method` | `entra_integrated` | Entra integrated token-based authentication; distinct from ordinary SQL integrated authentication. |
| `mssql.authentication.method` | `managed_identity` | Managed-identity authentication mode, without exporting identity IDs. |
| `mssql.authentication.method` | `service_principal_secret` | Service-principal authentication using a secret. |
| `mssql.authentication.method` | `service_principal_certificate` | Service-principal authentication using a certificate; no certificate/path content. |
| `mssql.authentication.method` | `interactive` | Built-in interactive browser authentication. Not a built-in device-code mode. |
| `mssql.authentication.method` | `default_credential` | Default credential-chain mode. Do not guess which credential in the chain succeeded. |
| `mssql.authentication.method` | `access_token` | Application supplied an already-acquired token. No driver token-acquisition child is implied. |
| `mssql.authentication.method` | `access_token_callback` | Application callback supplies the token. Its internal mechanism is not inferred. |
| `mssql.authentication.method` | `unknown` | Mechanism not yet resolved or invalid/conflicting input prevents safe classification. Do not echo the invalid input. |
| `mssql.authentication.token_source` | `msal` | The driver actually invoked MSAL for acquisition. |
| `mssql.authentication.token_source` | `native` | Native authentication/token provider was invoked. |
| `mssql.authentication.token_source` | `managed_identity` | Managed-identity credential provider was invoked. |
| `mssql.authentication.token_source` | `default_credential` | Default credential chain was invoked; not a claim about its selected member. |
| `mssql.authentication.token_source` | `callback` | Application token callback was invoked. |

For example, `method=service_principal_certificate` and `token_source=msal` describe different aspects of the same acquisition. Both refer to **database authentication**, never exporter authentication.

**Outcomes, attempts, and transport settings**

| Attribute | Value(s) | Meaning / selection rule |
|---|---|---|
| `mssql.connection.outcome` | `failure` | Overall open failed, without proven timeout/cancellation as its terminal outcome. |
| `mssql.connection.outcome` | `timeout` | A confirmed timeout caused the terminal unsuccessful open. |
| `mssql.connection.outcome` | `canceled` | Confirmed cancellation/interruption terminated the open. |
| `mssql.connection.outcome` | `success` | Usable connection returned; used internally but successful-open records are excluded from MVP failure export. |
| `mssql.connection.attempt_reason` | `initial` | First endpoint-establishment attempt. |
| `mssql.connection.attempt_reason` | `retry` | Additional attempt following a failed attempt under retry policy. |
| `mssql.connection.attempt_reason` | `redirect` | New attempt follows server routing instructions, not an ordinary failure retry. |
| `mssql.connection.attempt_reason` | `failover` | Attempt deliberately switches to a known failover partner/target. Use this more specific value instead of `retry` when applicable. |
| `mssql.connection.attempt_outcome` | `failure`, `timeout`, `canceled`, `success` | Same outcome meanings applied to this endpoint attempt, not the overall open. Attempt success can still be followed by failed session initialization. |
| `mssql.connection.attempt_outcome` | `redirect` | Attempt produced routing instructions rather than the final usable connection. Not inherently an error. |
| `mssql.connection.encrypt` | `false`, `true`, `strict` | Validated effective encryption mode. `false` does not prove no TLS was used; server requirements/login protection still matter. `strict` requests the strict/TDS8 path, not proof negotiation succeeded. |
| `mssql.connection.application_intent` | `read_write`, `read_only` | Declared workload intent; not an observation of the SQL statements executed or guaranteed server routing. |
| `mssql.connection.transport_strategy` | `serial` | Effective connection strategy tries candidates sequentially. |
| `mssql.connection.transport_strategy` | `parallel` | Effective connection strategy races multiple address candidates. |
| `mssql.connection.transport_strategy` | `tnir` | Transparent Network IP Resolution strategy is active. Prefer this specific strategy label over its internal serial/parallel stages; emit the effective strategy if TNIR was disabled. |

**Failure phases — shared vocabulary**

`mssql.connection.failure_phase` on the terminal root and `mssql.error.phase` on an event use the following values. The phase is where the failure happened, not the outer wrapper's catch location. Phase child names already identify their work; there is no separate `mssql.connection.phase` attribute.

| Value | Meaning |
|---|---|
| `configuration` | Driver-owned input validation or local setup. |
| `instance_discovery` | Named-instance/SQL Browser discovery. A positively identified resolver failure within it may use `dns`. |
| `dns` | Required endpoint name resolution. |
| `socket_connect` | Socket creation/connection/selection and setup. |
| `prelogin` | TDS PRELOGIN exchange or validation. |
| `tls` | Secure-channel setup, handshake, certificate or ALPN validation. |
| `token_acquisition` | Identity-provider/callback acquisition before sending the token to SQL Server. |
| `login` | SQL login/authentication exchange and response handling. |
| `redirect` | Routing metadata/control-flow failure. A subsequent routed DNS/TLS failure keeps its actual DNS/TLS phase. |
| `initialize` | Required post-login session setup before returning a usable connection. |
| `unknown` | Available evidence cannot establish the phase. |
| `reset` | Pool-reset extension only; use on reset error events and the failed owning-operation root as defined below. Not a physical-open phase. |

**Error source and retry decision**

| Attribute | Value | Meaning / selection rule |
|---|---|---|
| `mssql.error.source` | `driver` | Driver-owned validation, protocol check, or internal failure. |
| `mssql.error.source` | `sql_server` | An actual server ERROR response supplies the error, not merely a matching vendor number. |
| `mssql.error.source` | `jvm` | Java runtime/API failure, such as directly observed resolver or allocation error. |
| `mssql.error.source` | `os` | Explicitly established operating-system failure/resource exhaustion surfaced through the runtime. Do not guess the OS cause from a generic Java wrapper. |
| `mssql.error.source` | `identity_provider` | Error positively attributed to authentication/token service or credential-provider processing. |
| `mssql.error.source` | `callback` | Error originated from the application callback and no more specific underlying source is established. |
| `mssql.error.source` | `external_pool` | Pool-owned acquisition failure; does not create a driver-open failure if the driver was never called. |
| `mssql.error.source` | `unknown` | Origin cannot be established from available evidence. |
| `mssql.error.retry_decision` | `retry_scheduled` | Driver decided to make another attempt; not proof that it actually started. |
| `mssql.error.retry_decision` | `not_retryable` | Applicable connection policy excludes this failure from retry. |
| `mssql.error.retry_decision` | `limit_reached` | Retry count is exhausted or configured to permit no retries. |
| `mssql.error.retry_decision` | `budget_exhausted` | Insufficient remaining applicable time budget for another retry. |
| `mssql.error.retry_decision` | `canceled` | Cancellation prevented further attempts. |

The source describes the origin of the observed error, not ultimate responsibility. Prefer the specific established source over a wrapper class; for example, an SQLServerException wrapping a confirmed server ERROR remains `sql_server`. Retry decisions are optional: omit when unknown, and record the actual stopping gate if several conditions could apply. None authorizes replay of a statement or COMMIT.

**Redirect, timeout, and reset event values**

| Attribute | Value(s) | Meaning |
|---|---|---|
| `mssql.connection.redirect.type` | `tds_routing`, `enhanced_routing` | Ordinary versus enhanced routing information actually received. No target name/address. |
| `mssql.timeout.phase` | Applicable failure-phase value above | Phase whose wait expired. |
| `mssql.timeout.kind` | `login_budget` | Overall connection/login budget expiration is established. |
| `mssql.timeout.kind` | `socket_read` | Socket read wait expired. |
| `mssql.timeout.kind` | `socket_connect` | An explicitly observed socket connection deadline expired. |
| `mssql.timeout.kind` | `socket_selection` | The parallel connection-selection wait expired before any candidate supplied a result; distinct from socket reads. |
| `mssql.timeout.kind` | `token_request` | Token provider/callback request deadline expired. |
| `mssql.timeout.kind` | `routing_budget` | Routing transition exhausted its applicable connection budget. |
| `mssql.timeout.kind` | `unknown` | Timeout is established but the responsible budget cannot be identified. |
| `mssql.connection.reset.mechanism` | `tds_resetconnection` | This reset path uses the TDS reset flag, not a literal procedure invocation. |
| `mssql.connection.reset.dispatch` | `pending` | Reset marked locally but no successful reset-bearing transmission observed. |
| `mssql.connection.reset.dispatch` | `sent` | Successful reset-bearing transmission observed; does not itself prove server completion. |
| `mssql.connection.reset.dispatch` | `unknown` | Dispatch outcome uncertain, for example after a partial-write failure. |
| `mssql.connection.reset.outcome` | `deferred` | Synchronous reset handling ended while server dispatch remains pending; not a reset failure. |
| `mssql.connection.reset.outcome` | `success` | Synchronous driver-observed reset/initialization completed. Not isolated server reset timing. |
| `mssql.connection.reset.outcome` | `failure` | The reset operation failed; the pool may still recover by replacing the physical connection. |

**Not enums:** `error.type`, `exception.type`, namespaced `mssql.error.code`, SQLState, version strings, GUIDs, and numeric codes/counts are not closed lists of the examples shown. `mssql.error.category` uses the **16 canonical categories in the Summary**. `mssql.retry.reason` is described as bounded in this proposal, but its cross-driver value registry must be finalized before implementation; it must not become arbitrary message text. Native OTel `kind` and `status` are separate from these custom attributes: this proposal uses `CLIENT`/`INTERNAL` kinds and `ERROR`/`UNSET` status, not custom string replacements for native fields.

### 3. Child phases in execution order

Create only phases that execute, never zero-duration placeholders. Children share the open's native trace identity, not its attribute map; the connection GUID remains on the root only. The child span name supplies its phase. The table follows the usual TDS 7.x path.

| Span name | Parent | Exact measured work / applicability |
|---|---|---|
| `mssql.driver.connection.configuration` | Open | Driver-owned validation and local setup, including failures before any network attempt. |
| `mssql.driver.connection.attempt` | Open | One endpoint-establishment attempt, from discovery/transport setup through completed login processing; one per initial, retry, redirect, or failover attempt. **Backoff is outside the attempt, inside open.** |
| `mssql.driver.connection.instance_discovery` | Attempt | SQL Browser request/response and endpoint discovery when named-instance resolution is required; not pool wait. |
| `mssql.driver.connection.dns` | Attempt, or instance discovery for Browser resolution | Actual driver-invoked remote-endpoint resolution. Omit when no resolver work runs; exclude local workstation-name lookup. |
| `mssql.driver.connection.socket_connect` | Attempt | Socket connection/selection and required socket setup, excluding separately timed DNS. Aggregate parallel candidates in one MVP child without candidate addresses. |
| `mssql.driver.connection.prelogin` | Attempt | Build/send PRELOGIN and receive/validate its response, when executed. |
| `mssql.driver.connection.tls` | Attempt | TLS setup, handshake, peer validation, applicable ALPN, and secure-channel activation, when executed; not synonymous with prelogin. |
| `mssql.driver.connection.login` | Attempt | LOGIN7/authentication exchange through acceptance, rejection, or redirect response, after required transport setup; excludes discovery/TCP/backoff. |
| `mssql.driver.connection.token_acquisition` | Login, or actual owning phase | Provider/callback invocation until token return/failure. **Excludes token transmission and reading SQL Server's reply.** Omit for a pre-supplied token. |
| `mssql.driver.connection.initialize` | Open | Required post-login session initialization before a usable connection is returned. Failure still fails open; record no SQL text. |

- **TDS 7.x:** socket → prelogin → TLS → login. **Strict/TDS8:** socket → TLS → prelogin → login.
- Token acquisition may be nested inside login: do not add parent login duration and token-child duration to compute total time.
- Retries/redirects are decision **events**, followed by real attempt children. Do not invent zero-duration spans; any future redirect span must measure actual work without duplicating the next attempt.
- **Recovery boundary:** idle recovery is not a fresh application open. Beyond this MVP, use a distinct `mssql.driver.connection.reconnect` with its own children and `mssql.driver.reconnect` events, correlated to the triggering operation. Never attach COMMIT/ROLLBACK to an ended open span. External pool exhaustion before driver entry belongs on pool-owned acquisition, using the normalized category when supported by evidence.

### 4. Events and failures

#### Error placement and overall outcome

Use **`mssql.driver.error`**, not an error child span: the event has a timestamp but no duration.

- Attach one compact event per observed failure to the lowest instrumented phase that owns it. If no phase child exists, attach it to the open span. Do not add a duplicate root error event for a child-origin failure.
- Root-only views use native status and the root's `mssql.connection.outcome`, `mssql.connection.failure_phase`, `mssql.error.category`, and `error.type` attributes for the final failure.
- Mark the failing child and enclosing failed attempt `ERROR`; mark the root `ERROR` only for terminal failure. Successful preceding children stay `UNSET`.
- Retry/redirect decisions are separate events recording actual decisions. Successful retry does not fail the root. A retained failed trace includes earlier failed attempts; the root outcome determines whether the overall open failed, without a terminal Boolean on each error event.
- Count terminal failures once per open using its final outcome, **not** per error event or failed child. Correlate events through native trace/span IDs and timestamps, plus `mssql.connection.attempt` on the owning span when available; no separate error ID is needed.

On terminal failure, separately set the root's native status `ERROR`, `mssql.connection.outcome` (`failure`, `timeout`, or `canceled`), failure phase, category, and `error.type`; keep the error event on its owning phase. Category describes cause, not outcome: budget exhaustion does not automatically replace a specific cause with `timeout`. Record `mssql.connection.budget_exhausted` and the actual retry decision when known. Retry-list membership does not prove transience; missing evidence stays unknown.

#### Compact error-event schema — seven MVP fields

The MVP allowlist has **three required fields and four optional fields**, not seven mandatory values. Event name/timestamp and owning-span correlation are native context, not additional error attributes.

| Error-event attribute | Type | Requirement / precise value |
|---|---|---|
| `mssql.error.category` | String | Required; same normalized category as the failing span. |
| `mssql.error.phase` | String | Required; original failing phase, not merely the outer catch location. Retained for standalone event queries even though the owning phase span's name also identifies the phase. |
| `mssql.error.source` | String | Required; `driver`, `sql_server`, `jvm`, `os`, `identity_provider`, `callback`, `external_pool`, or `unknown`. External-pool events belong on the pool-owned span if the driver was never called. |
| `mssql.error.code` | String | Optional; namespaced primary code, e.g. `sqlserver:18456`, `jdbc:R_invalidPortNumber`, `jdbc_driver:5`. Use only an actual code/key captured at its source; omit when unavailable. Do not manufacture an OS code or reconstruct a resource key from localized text. |
| `mssql.error.retry_decision` | String | Optional; actual connection-establishment decision when known: `retry_scheduled`, `not_retryable`, `limit_reached`, `budget_exhausted`, `canceled`. Omit when unknown; never a statement/transaction replay guarantee. |
| `exception.type` | String | Optional; actual exception class captured at the failure origin. Do not fabricate a discarded cause type. |
| `mssql.error.message` | String | Optional; approved sanitized/normalized diagnostic text, capped at 1,024 UTF-8 bytes. Omit if safety cannot be established; never serialize Throwable text blindly. |

**Optional diagnostic enrichment**, only where actually available:

| Error-event attribute | Type | Availability / meaning |
|---|---|---|
| `mssql.error.sql_state` | String | Actual nonempty JDBC SQLState. No assumption that authentication is `28000` or timeout is `HYT00`. |
| `mssql.error.server_state` | Int64 | Numeric state from an actual parsed SQLServerError. |
| `mssql.error.server_severity` | Int64 | Severity from an actual parsed SQLServerError. |
| `mssql.error.driver_code` | Int64 | Actual internal driver code captured inside the core; not the JDBC vendor number. |

The primary namespaced code carries the server number or driver resource key without a second copy. Use `sqlserver:` only for a confirmed server error; a local Kerberos exception with vendor 18456 is not sufficient. `error.type` stays on the root/failing span rather than repeating it on the error event. Do not add error IDs, role/terminal flags, reason, vendor/server-number duplicates, transience/retryability claims, evidence/cause/wrapper fields, or message-processing flags to this compact schema.

An optional standard **`exception`** event may be recorded on the owning span only in a separately approved diagnostic mode: `exception.type`, sanitized `exception.message`, and optionally sanitized, bounded `exception.stacktrace` (maximum 4,096 UTF-8 bytes). This event and stack traces are **off by default**; do not duplicate the error event's message unless separately approved. Correlate through the owning trace/span, timestamps, and attempt context, without an error ID or another failure count. Do not traverse lazy SQLException chains in a way that triggers additional server reads.

#### Authentication, retry, redirect, and timeout events

| Event name | Placement | Exact allowed event-specific attributes |
|---|---|---|
| `mssql.driver.authentication` | Token/login child | `mssql.authentication.method` (string), `mssql.authentication.token_source` (string when known); never token/user/client-secret values. |
| `mssql.driver.retry` | Open | `mssql.connection.attempt` (int64: originating attempt index), `mssql.retry.attempt` (int64: next attempt index), `mssql.retry.reason` (bounded string), `mssql.retry.delay` (double seconds), `error.type` (string). Record only an actual retry-scheduling decision; correlate to the failed attempt through trace identity, parent references, and timestamps. A decision event does not imply the next attempt actually began. |
| `mssql.driver.redirect` | Open | `mssql.connection.attempt` (int64 origin index), `mssql.connection.redirect.index` (int64), `mssql.connection.redirect.type` (string: `tds_routing` or `enhanced_routing`), `mssql.connection.endpoint_role` (string: `redirect_target`). No target hostname, IP, database, or URL. |
| `mssql.driver.timeout` | Failing phase, or root if no phase | `mssql.timeout.phase` (string), `mssql.timeout.value` (double seconds when known), `mssql.timeout.kind` (string: `login_budget`, `socket_connect`, `socket_selection`, `socket_read`, `token_request`, `routing_budget`, `unknown`), `mssql.connection.attempt` (int64 when available), `error.type` (string). Correlate through native trace/span IDs, timestamps, and attempt context. Companion diagnostic event, not another overall failure. |

### 5. Pooled acquisition and reset — separate extension

**Logical borrow/reset is not a new physical open.** This diagnostic extension uses the active acquisition/caller context, never the original already-ended open as its parent.

#### What reset measures

The driver does not issue literal `EXEC sp_reset_connection` here. [Pooled reborrow](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPooledConnection.java#L110-L162) calls [resetPooledConnection()](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java#L2264-L2279): mark the next TDS packet for reset, reset local state/caches, and reapply configured session properties. The [writer sets the reset flag](../src/main/java/com/microsoft/sqlserver/jdbc/IOBuffer.java#L3647-L3651) and [consumes it while writing a packet](../src/main/java/com/microsoft/sqlserver/jdbc/IOBuffer.java#L4601-L4627). Server diagnostics may call the resulting reset `sp_reset_connection`.

**Setting a flag is not a round trip.** Session-property commands may transmit it during acquisition; otherwise it waits for a later request. Timing the Java method alone does not necessarily measure completed server reset.

| Span | Owner / parent | Lifetime and interpretation |
|---|---|---|
| `mssql.pool.connection.acquire` | Pool instrumentation; proposed name, under current application operation | Borrow request through usable logical handle or acquisition failure, including pool wait. The driver cannot measure an external queue without pool instrumentation. |
| `mssql.driver.connection.reset` | Driver; active acquisition span when available, otherwise current caller context | `INTERNAL`; local reset through completion/failure of synchronous reset-bearing initialization. If nothing is dispatched, end as deferred, not server-reset success. |
| `mssql.driver.connection.open` | Driver; acquisition child only when a physical connection is created | Ordinary physical-open contract; reuse alone creates no open span. |

External pools may use the JDBC pooled API or restore state via setters, rollback, or validation. Emit reset only for an observed reset path, not every borrow. A reset error is an event inside the reset span, not another child span.

#### Reset-specific attribute allowlist

These keys extend the span/event schemas **only for this extension**, never metric labels. Error events retain the compact error-event schema, optional diagnostic enrichment, and privacy rules.

| Attribute | Type | Placement / meaning |
|---|---|---|
| `mssql.connection.id` | Int64 | Optional diagnostic only on the owning operation root, when allocated and diagnostic enrichment is enabled; not mandatory, not on nested reset spans or events. |
| `mssql.connection.guid` | String | Owning operation root only; retain the successful physical-open GUID internally across borrows and later separately rooted reset/acquisition/recovery operations. Never repeat on nested reset/phase spans or events; a replacement physical connection gets a new GUID. |
| `mssql.connection.acquisition.id` | String | Proposed opaque per-borrow ID shared by acquire/reset and deferred-reset diagnostics, distinct from physical GUID and trace ID. |
| `mssql.connection.reset.mechanism` | String | `tds_resetconnection` for this path; no procedure/SQL text. |
| `mssql.connection.reset.dispatch` | String | `pending`, `sent`, or `unknown`. Mark sent only after observed successful transmission, not flag clearing; partial-write failure may leave dispatch unknown. |
| `mssql.connection.reset.outcome` | String | `deferred`, `success`, or `failure`. Success means driver-observed synchronous reset/initialization completed, not separately measured server reset CPU time. |
| `mssql.connection.failure_phase` | String | Allow `reset` only on the terminal failed owning operation root, never on a nested reset/phase span. The reset span name identifies the phase; its error event retains `mssql.error.phase=reset`. |
| `mssql.error.category`, `error.type` | String | Same evidence-based span error contract; reset is a phase, not a replacement for timeout/network/server categories. |

Retain the physical GUID internally even when successful opens are not exported. Explicitly propagate correlation context across borrows and asynchronous work; parenting/matching IDs do not copy attribute maps. Place the GUID on the acquisition operation root, or on a separately rooted reset operation when there is no acquisition owner, not both. Nested reset spans require a root lookup. Independently rooted later operations may carry approved user-agent metadata once on their root, not each reset child. Reset does not create a new connection attempt, so it does not export `mssql.connection.client_connection_id`; any future reset-specific TDS diagnostic enrichment requires separate approval, not default duplication. A genuine replacement physical open uses the ordinary attempt-only TDS UUID contract.

#### Deferred/asynchronous attribution and failure counting

- **No dispatch:** end acquisition-time reset with native status `UNSET`, dispatch `pending`, outcome `deferred`. A usable logical handle may still be returned; pending reset is not failure.
- **Later dispatch:** if request instrumentation is enabled, attach timestamped `mssql.driver.connection.reset_dispatch` to the actual request span. Allowed attributes are `mssql.connection.acquisition.id`, `mssql.connection.reset.mechanism`, and `mssql.connection.reset.dispatch` (Strings), using the availability and value rules above. Connection GUID lookup requires the owning operation root; neither physical driver ID nor TDS UUID is duplicated on the event. Carry the captured acquisition ID/context across asynchronous work. After acquisition ends, do not reopen it, append a late event, or fabricate a child of it: correlate the later request through the acquisition ID and, when available, a **native OTel link** to its captured span context.
- **Later request fails:** reset and request execution share the request. Do not report the whole request duration as reset latency or assume every request error is a reset error. Without specific reset evidence, retain dispatch context and use the actual request failure phase/category.
- **Synchronous reset fails:** mark reset `ERROR`, set `mssql.connection.reset.outcome=failure`, and add one compact `mssql.driver.error` with `mssql.error.phase=reset` and actual available error fields. Set `mssql.connection.failure_phase=reset` only on the terminal failed owning operation root, not a nested reset span. Acquisition fails only if no usable handle is returned; discard/replacement can make acquisition succeed despite the failed reset child. The acquisition's own final status/outcome determines its result; do not copy the reset failure into a duplicate acquisition error event.
- **Count and retain separately:** reset/borrow failures do not increment physical `mssql.driver.connection.failure.count` or `mssql.driver.connection.timeout.count`. Any reset/acquire counters must be separately defined and attribute-free; genuine replacement physical-open failures use physical-open counters once. Select failed reset/acquisition operations separately for retention, or they would disappear when no open occurs. Successful acquisition/reset latency and statement instrumentation remain outside the physical-open MVP.

### 6. Export, privacy, and metrics

#### Privacy across the driver telemetry stream

Apply policy to resources, scope metadata, spans, events, and correlated logs. Prohibit host names, IPs, user names, raw database names/SQL, full connection strings, token/authorization headers, passwords, certificate/key paths, SPNs/authority URLs, exporter endpoint URLs, ARM IDs containing resource names, and unfiltered exception/property/baggage or arbitrary user-agent payloads. Omit `server.address`, `server.port` if not required by the approved schema, `network.peer.address`, `db.namespace`, `db.query.text`, and `azure.resource.id`; exclude automatic `host.name` and IP/endpoint attributes. Hashing an endpoint does not automatically approve it.

The fixed driver-generated user-agent is the specific metadata exception, not permission to copy arbitrary headers/property maps. Its seven fields intentionally contain no hostname, IP, username, credentials, or endpoints, but JVM system properties can be overridden. **Bounded character sanitization is not privacy redaction:** validate/approve this format and omit it if policy cannot be met. With a shared customer SDK, filter the driver stream without altering unrelated application telemetry.

#### Metrics and failure-only export

| Signal / concern | MVP behavior |
|---|---|
| `mssql.driver.connection.failure.count` | Int64 counter, add 1 exactly once when a driver-owned open fails to return a usable connection, including terminal timeout/cancellation. **No data-point attributes.** |
| `mssql.driver.connection.timeout.count` | Int64 counter, add 1 when that terminal outcome is `timeout`. Subset of failure count; no attributes. Intermediate retry timeouts alone do not increment it. |
| Metric dimensions | No category, connection ID, authentication, endpoint, or outcome labels. Category/authentication breakdown comes from retained spans/events. Resource/scope metadata remains separately governed. |
| Successful open | Export no successful-open diagnostic trace or latency, including opens with handled intermediate failures. Keep context until final outcome is known, then release successful-open records. |
| Failure trace retention | Outcome is unknown when phases start. Use bounded per-open diagnostic buffering with IDs/timestamps/context captured at start, and a nonblocking failure-aware processing path that emits the root and its retained children together after failure. This is an implementation requirement, not behavior provided by a head sampler alone. |
| Sampling | Do not apply early 5% head sampling and still promise complete failure capture. Explicitly configure the diagnostic pipeline to retain failure records; if an application sampler refuses recording, honor that integration policy and document reduced coverage. Never silently override upstream sampling or change IDs after failure. |
| Limits | Provisional limits: **128 spans and 256 events per open**, subject to load testing. On overflow set the root's truncation/dropped-count attributes from the full allowlist. Reserve capacity for the root, final failing phase and its ancestor chain, and the single error event for that final failure (root-attached only when no phase child exists); also bound global queued bytes/operations. |
| Early failures | Export destination and auth must be available independently of successful SQL negotiation. DNS/TCP/TLS failures cannot rely solely on a destination delivered by a later feature ACK. |
| Export | Asynchronous bounded queue; never force-flush or perform exporter token/network work on the connection-return path. Application-owned SDK is not replaced or shut down by the driver. |
| Reliability | Never replace SQL results/exceptions or broadly swallow JVM errors. Measure bounded diagnostic overhead. Queue overflow, OOM, crashes, and collector outages can lose telemetry; counters are also best-effort. **Severe resource failure cannot guarantee capture.** |

### 7. Worked examples

Each example has **one synthetic decoded JSON envelope with a `spans` array containing every span**: **4 spans for DNS failure**, **8 spans for SQL login rejection**. These are not captured production output or literal OTLP JSON. Subspans are separate records with `parent_span_id` references, not nested OTLP children. Every nonroot parent is present in the same array; attributes are explicit, not inherited. Error entries in the trees are attached events, not extra spans.

Values retain JSON/OTel types; an OTLP encoder supplies its native field names, epoch-nanosecond times, and `stringValue`, `intValue`, `doubleValue`, and `boolValue` wrappers. Times and GUIDs are synthetic. Both envelopes use the application-owned resource and `com.microsoft.sqlserver.jdbc` instrumentation scope (version `13.5.0.0-preview`) described in section 2; no application service identity is invented. The privacy-approved user-agent, connection GUID, and terminal failure phase appear only on each root. Phase names and native parent links replace redundant phase/identity attributes; root/attempt lookups are required, not implicit inheritance. No process-local connection ID or reachability attribute is included. Empty `events` arrays are explicit; each example has exactly one compact error event, on the failing child, and no root error event.

#### DNS failure before reaching SQL Server

**Scenario:** SQL-password connection, first endpoint resolution fails, and configured policy permits no retry. No socket, prelogin, TLS, or login work ran. Classification is `name_resolution`, not firewall or server outage. The logical GUID is available; the TDS ClientConnectionId is not yet allocated.

```text
mssql.driver.connection.open                 CLIENT   ERROR  8 ms
├─ mssql.driver.connection.configuration     INTERNAL UNSET  1 ms
└─ mssql.driver.connection.attempt           INTERNAL ERROR  6 ms
   └─ mssql.driver.connection.dns            INTERNAL ERROR  6 ms
      └─ mssql.driver.error  [event, name_resolution]
```

All **4 span records** follow. Configuration runs at 0–1 ms, attempt/DNS at 1–7 ms, and final failure handling ends the root at 8 ms.

```json
{
	"spans": [
		{
			"trace_id": "11111111111111111111111111111111",
			"span_id": "1111111111111101",
			"name": "mssql.driver.connection.open", "kind": "CLIENT",
			"start_time": "2026-09-15T10:00:00.000000000Z",
			"end_time": "2026-09-15T10:00:00.008000000Z", "status": "ERROR",
			"attributes": {
				"mssql.telemetry.schema.version": "1.0",
				"mssql.driver.user_agent.original": "1|MS-JDBC|13.5.0.0-preview|amd64|Windows|Windows 11 10.0|OpenJDK 64-Bit Server VM 21.0.4",
				"db.system.name": "microsoft.sql_server",
				"mssql.connection.guid": "11111111-1111-4111-8111-111111111111",
				"mssql.connection.origin": "datasource",
				"mssql.connection.outcome": "failure",
				"mssql.connection.failure_phase": "dns",
				"mssql.authentication.method": "sql_password",
				"mssql.connection.encrypt": "true",
				"mssql.connection.trust_server_certificate": false,
				"mssql.connection.application_intent": "read_write",
				"mssql.connection.multi_subnet_failover": false,
				"mssql.connection.transparent_network_ip_resolution": false,
				"mssql.connection.login_timeout": 30.0,
				"mssql.connection.socket_timeout": 0.0,
				"mssql.connection.connect_retry_count": 0,
				"mssql.connection.connect_retry_interval": 10.0,
				"mssql.connection.attempt_count": 1,
				"mssql.connection.retry_count": 0,
				"mssql.connection.redirect_count": 0,
				"mssql.error.category": "name_resolution",
				"error.type": "java.net.UnknownHostException"
			},
			"events": []
		},
		{
			"trace_id": "11111111111111111111111111111111",
			"span_id": "1111111111111102", "parent_span_id": "1111111111111101",
			"name": "mssql.driver.connection.configuration", "kind": "INTERNAL",
			"start_time": "2026-09-15T10:00:00.000000000Z",
			"end_time": "2026-09-15T10:00:00.001000000Z", "status": "UNSET",
			"attributes": {
				"db.system.name": "microsoft.sql_server"
			},
			"events": []
		},
		{
			"trace_id": "11111111111111111111111111111111",
			"span_id": "1111111111111103", "parent_span_id": "1111111111111101",
			"name": "mssql.driver.connection.attempt", "kind": "INTERNAL",
			"start_time": "2026-09-15T10:00:00.001000000Z",
			"end_time": "2026-09-15T10:00:00.007000000Z", "status": "ERROR",
			"attributes": {
				"db.system.name": "microsoft.sql_server",
				"mssql.connection.attempt": 1,
				"mssql.connection.attempt_reason": "initial",
				"mssql.connection.attempt_outcome": "failure",
				"mssql.connection.endpoint_role": "database"
			},
			"events": []
		},
		{
			"trace_id": "11111111111111111111111111111111",
			"span_id": "1111111111111104", "parent_span_id": "1111111111111103",
			"name": "mssql.driver.connection.dns", "kind": "INTERNAL",
			"start_time": "2026-09-15T10:00:00.001000000Z",
			"end_time": "2026-09-15T10:00:00.007000000Z", "status": "ERROR",
			"attributes": {
				"db.system.name": "microsoft.sql_server",
				"mssql.connection.attempt": 1,
				"mssql.connection.endpoint_role": "database",
				"mssql.error.category": "name_resolution",
				"error.type": "java.net.UnknownHostException"
			},
			"events": [{
				"name": "mssql.driver.error",
				"time": "2026-09-15T10:00:00.007000000Z",
				"attributes": {
					"mssql.error.category": "name_resolution",
					"mssql.error.phase": "dns",
					"mssql.error.source": "jvm",
					"mssql.error.retry_decision": "limit_reached",
					"exception.type": "java.net.UnknownHostException",
					"mssql.error.message": "The required database endpoint name could not be resolved."
				}
			}]
		}
	]
}
```

DNS evidence must be captured at the resolver site **before** TCP conversion loses its type. Without that instrumentation, classify conservatively rather than reconstructing a missing cause. No namespaced code or JDBC/server diagnostics are available at this resolver origin, so they are omitted; the later TCP wrapper is not a second error event. The retained event includes the actual no-retry decision once known, while its timestamp remains the failure time. Resolver failure alone does not establish transience. No hostname, IP, username, or fabricated server error is shipped. Metrics: failure **+1**, timeout **+0**, both with empty attributes.

#### SQL Server rejects login with 18456

**Scenario:** SQL-password connection reaches SQL Server over TDS 7.x; TLS completes; the server returns error number **18456**, numeric state **1**, severity **14**. No retry is taken. These are assumed received values for the example, not values guaranteed for every 18456. Authentication category does not assert that the password was wrong.

```text
mssql.driver.connection.open                 CLIENT   ERROR  120 ms
├─ mssql.driver.connection.configuration     INTERNAL UNSET    2 ms
└─ mssql.driver.connection.attempt           INTERNAL ERROR   116 ms
   ├─ mssql.driver.connection.dns            INTERNAL UNSET    3 ms
   ├─ mssql.driver.connection.socket_connect INTERNAL UNSET   12 ms
   ├─ mssql.driver.connection.prelogin       INTERNAL UNSET    8 ms
   ├─ mssql.driver.connection.tls            INTERNAL UNSET   30 ms
   └─ mssql.driver.connection.login          INTERNAL ERROR   63 ms
      └─ mssql.driver.error  [event, authentication, sqlserver:18456]
```

All **8 span records** follow. There is no token-acquisition child for SQL-password authentication and no initialize child after rejection. The 116 ms attempt covers the five children, plus 2 ms configuration and 2 ms final failure handling in the 120 ms root. Phase intervals are DNS 2–5, socket 5–17, prelogin 17–25, TLS 25–55, and login 55–118 ms. The actual TDS connection UUID appears only on the attempt once allocated, never on the root, phase children, or error event. A phase/event query must look up the owning attempt through native parent links; the UUID does not imply a server response during DNS.

```json
{
	"spans": [
		{
			"trace_id": "22222222222222222222222222222222",
			"span_id": "2222222222222201",
			"name": "mssql.driver.connection.open", "kind": "CLIENT",
			"start_time": "2026-09-15T10:01:00.000000000Z",
			"end_time": "2026-09-15T10:01:00.120000000Z", "status": "ERROR",
			"attributes": {
				"mssql.telemetry.schema.version": "1.0",
				"mssql.driver.user_agent.original": "1|MS-JDBC|13.5.0.0-preview|amd64|Windows|Windows 11 10.0|OpenJDK 64-Bit Server VM 21.0.4",
				"db.system.name": "microsoft.sql_server",
				"mssql.connection.guid": "22222222-2222-4222-8222-222222222222",
				"mssql.connection.origin": "datasource",
				"mssql.connection.outcome": "failure",
				"mssql.connection.failure_phase": "login",
				"mssql.authentication.method": "sql_password",
				"mssql.connection.encrypt": "true",
				"mssql.connection.trust_server_certificate": false,
				"mssql.connection.application_intent": "read_write",
				"mssql.connection.multi_subnet_failover": false,
				"mssql.connection.transparent_network_ip_resolution": false,
				"mssql.connection.login_timeout": 30.0,
				"mssql.connection.socket_timeout": 0.0,
				"mssql.connection.connect_retry_count": 0,
				"mssql.connection.connect_retry_interval": 10.0,
				"mssql.connection.attempt_count": 1,
				"mssql.connection.retry_count": 0,
				"mssql.connection.redirect_count": 0,
				"mssql.error.category": "authentication",
				"error.type": "sqlserver.18456"
			},
			"events": []
		},
		{
			"trace_id": "22222222222222222222222222222222",
			"span_id": "2222222222222202", "parent_span_id": "2222222222222201",
			"name": "mssql.driver.connection.configuration", "kind": "INTERNAL",
			"start_time": "2026-09-15T10:01:00.000000000Z",
			"end_time": "2026-09-15T10:01:00.002000000Z", "status": "UNSET",
			"attributes": {
				"db.system.name": "microsoft.sql_server"
			},
			"events": []
		},
		{
			"trace_id": "22222222222222222222222222222222",
			"span_id": "2222222222222203", "parent_span_id": "2222222222222201",
			"name": "mssql.driver.connection.attempt", "kind": "INTERNAL",
			"start_time": "2026-09-15T10:01:00.002000000Z",
			"end_time": "2026-09-15T10:01:00.118000000Z", "status": "ERROR",
			"attributes": {
				"db.system.name": "microsoft.sql_server",
				"mssql.connection.client_connection_id": "22222222-2222-4222-8222-222222222223",
				"mssql.connection.attempt": 1,
				"mssql.connection.attempt_reason": "initial",
				"mssql.connection.attempt_outcome": "failure",
				"mssql.connection.endpoint_role": "database"
			},
			"events": []
		},
		{
			"trace_id": "22222222222222222222222222222222",
			"span_id": "2222222222222204", "parent_span_id": "2222222222222203",
			"name": "mssql.driver.connection.dns", "kind": "INTERNAL",
			"start_time": "2026-09-15T10:01:00.002000000Z",
			"end_time": "2026-09-15T10:01:00.005000000Z", "status": "UNSET",
			"attributes": {
				"db.system.name": "microsoft.sql_server",
				"mssql.connection.attempt": 1,
				"mssql.connection.endpoint_role": "database"
			},
			"events": []
		},
		{
			"trace_id": "22222222222222222222222222222222",
			"span_id": "2222222222222205", "parent_span_id": "2222222222222203",
			"name": "mssql.driver.connection.socket_connect", "kind": "INTERNAL",
			"start_time": "2026-09-15T10:01:00.005000000Z",
			"end_time": "2026-09-15T10:01:00.017000000Z", "status": "UNSET",
			"attributes": {
				"db.system.name": "microsoft.sql_server",
				"mssql.connection.attempt": 1,
				"mssql.connection.endpoint_role": "database"
			},
			"events": []
		},
		{
			"trace_id": "22222222222222222222222222222222",
			"span_id": "2222222222222206", "parent_span_id": "2222222222222203",
			"name": "mssql.driver.connection.prelogin", "kind": "INTERNAL",
			"start_time": "2026-09-15T10:01:00.017000000Z",
			"end_time": "2026-09-15T10:01:00.025000000Z", "status": "UNSET",
			"attributes": {
				"db.system.name": "microsoft.sql_server",
				"mssql.connection.attempt": 1,
				"mssql.connection.endpoint_role": "database"
			},
			"events": []
		},
		{
			"trace_id": "22222222222222222222222222222222",
			"span_id": "2222222222222207", "parent_span_id": "2222222222222203",
			"name": "mssql.driver.connection.tls", "kind": "INTERNAL",
			"start_time": "2026-09-15T10:01:00.025000000Z",
			"end_time": "2026-09-15T10:01:00.055000000Z", "status": "UNSET",
			"attributes": {
				"db.system.name": "microsoft.sql_server",
				"mssql.connection.attempt": 1,
				"mssql.connection.endpoint_role": "database"
			},
			"events": []
		},
		{
			"trace_id": "22222222222222222222222222222222",
			"span_id": "2222222222222208", "parent_span_id": "2222222222222203",
			"name": "mssql.driver.connection.login", "kind": "INTERNAL",
			"start_time": "2026-09-15T10:01:00.055000000Z",
			"end_time": "2026-09-15T10:01:00.118000000Z", "status": "ERROR",
			"attributes": {
				"db.system.name": "microsoft.sql_server",
				"mssql.connection.attempt": 1,
				"mssql.authentication.method": "sql_password",
				"mssql.error.category": "authentication",
				"error.type": "sqlserver.18456"
			},
			"events": [{
				"name": "mssql.driver.error",
				"time": "2026-09-15T10:01:00.118000000Z",
				"attributes": {
					"mssql.error.category": "authentication",
					"mssql.error.phase": "login",
					"mssql.error.source": "sql_server",
					"mssql.error.code": "sqlserver:18456",
					"mssql.error.retry_decision": "not_retryable",
					"exception.type": "com.microsoft.sqlserver.jdbc.SQLServerException",
					"mssql.error.message": "SQL Server rejected the login. The client-visible error does not establish the specific credential or access failure.",
					"mssql.error.sql_state": "S0001",
					"mssql.error.server_state": 1,
					"mssql.error.server_severity": 14,
					"mssql.error.driver_code": 2
				}
			}]
		}
	]
}
```

`S0001` in this example follows the non-X/Open mapping of received server state 1; it is **not a generic authentication SQLState**. With X/Open enabled the corresponding 18456 mapping is `08001`. No JDBC resource key is invented for the server's message, and no raw user name is shipped. Metrics: failure **+1**, timeout **+0**, with empty data-point attributes.

#### Successful physical connection — illustration only, not exported in MVP

**Scenario:** a SQL-password connection completes configuration, DNS, TCP, TDS 7.x prelogin, TLS, login, and session initialization. A usable physical connection is returned in **120 ms**. No token-acquisition span is needed for SQL-password authentication.

> **Illustration only. None of the spans in this successful-connection example are shipped by the MVP.** The example explains the normal phase hierarchy and contrasts success with the two exported failure scenarios. It is not a requirement to export successful connections or successful-connection latency.

Any temporary diagnostic records for a successful open are released without export. Adding successful-connection export would require an explicit future scope decision, not merely changing the example's status. Failure counter **+0**; timeout counter **+0**.

```text
mssql.driver.connection.open                 CLIENT   UNSET  120 ms
├─ mssql.driver.connection.configuration     INTERNAL UNSET    2 ms
├─ mssql.driver.connection.attempt           INTERNAL UNSET  108 ms
│  ├─ mssql.driver.connection.dns            INTERNAL UNSET    3 ms
│  ├─ mssql.driver.connection.socket_connect INTERNAL UNSET   12 ms
│  ├─ mssql.driver.connection.prelogin       INTERNAL UNSET    8 ms
│  ├─ mssql.driver.connection.tls            INTERNAL UNSET   30 ms
│  └─ mssql.driver.connection.login          INTERNAL UNSET   55 ms
└─ mssql.driver.connection.initialize        INTERNAL UNSET   10 ms
```

All **nine spans** are included below as separate records. Native status remains `UNSET` on success; `mssql.connection.outcome=success` explicitly describes the root result. There is **no** `mssql.connection.failure_phase`, `mssql.error.category`, `error.type`, or error event. Values and timestamps are synthetic, using the same decoded JSON format as the failure examples.

```json
{
	"spans": [
		{
			"trace_id": "33333333333333333333333333333333",
			"span_id": "3333333333333301",
			"name": "mssql.driver.connection.open", "kind": "CLIENT",
			"start_time": "2026-09-15T10:02:00.000000000Z",
			"end_time": "2026-09-15T10:02:00.120000000Z", "status": "UNSET",
			"attributes": {
				"mssql.telemetry.schema.version": "1.0",
				"mssql.driver.user_agent.original": "1|MS-JDBC|13.5.0.0-preview|amd64|Windows|Windows 11 10.0|OpenJDK 64-Bit Server VM 21.0.4",
				"db.system.name": "microsoft.sql_server",
				"mssql.connection.guid": "33333333-3333-4333-8333-333333333333",
				"mssql.connection.origin": "datasource",
				"mssql.connection.outcome": "success",
				"mssql.authentication.method": "sql_password",
				"mssql.connection.encrypt": "true",
				"mssql.connection.trust_server_certificate": false,
				"mssql.connection.application_intent": "read_write",
				"mssql.connection.multi_subnet_failover": false,
				"mssql.connection.transparent_network_ip_resolution": false,
				"mssql.connection.login_timeout": 30.0,
				"mssql.connection.socket_timeout": 0.0,
				"mssql.connection.connect_retry_count": 0,
				"mssql.connection.connect_retry_interval": 10.0,
				"mssql.connection.attempt_count": 1,
				"mssql.connection.retry_count": 0,
				"mssql.connection.redirect_count": 0
			},
			"events": []
		},
		{
			"trace_id": "33333333333333333333333333333333",
			"span_id": "3333333333333302", "parent_span_id": "3333333333333301",
			"name": "mssql.driver.connection.configuration", "kind": "INTERNAL",
			"start_time": "2026-09-15T10:02:00.000000000Z",
			"end_time": "2026-09-15T10:02:00.002000000Z", "status": "UNSET",
			"attributes": { "db.system.name": "microsoft.sql_server" },
			"events": []
		},
		{
			"trace_id": "33333333333333333333333333333333",
			"span_id": "3333333333333303", "parent_span_id": "3333333333333301",
			"name": "mssql.driver.connection.attempt", "kind": "INTERNAL",
			"start_time": "2026-09-15T10:02:00.002000000Z",
			"end_time": "2026-09-15T10:02:00.110000000Z", "status": "UNSET",
			"attributes": {
				"db.system.name": "microsoft.sql_server",
				"mssql.connection.attempt": 1,
				"mssql.connection.attempt_reason": "initial",
				"mssql.connection.attempt_outcome": "success",
				"mssql.connection.endpoint_role": "database",
				"mssql.connection.client_connection_id": "33333333-3333-4333-8333-333333333334"
			},
			"events": []
		},
		{
			"trace_id": "33333333333333333333333333333333",
			"span_id": "3333333333333304", "parent_span_id": "3333333333333303",
			"name": "mssql.driver.connection.dns", "kind": "INTERNAL",
			"start_time": "2026-09-15T10:02:00.002000000Z",
			"end_time": "2026-09-15T10:02:00.005000000Z", "status": "UNSET",
			"attributes": {
				"db.system.name": "microsoft.sql_server",
				"mssql.connection.attempt": 1,
				"mssql.connection.endpoint_role": "database"
			},
			"events": []
		},
		{
			"trace_id": "33333333333333333333333333333333",
			"span_id": "3333333333333305", "parent_span_id": "3333333333333303",
			"name": "mssql.driver.connection.socket_connect", "kind": "INTERNAL",
			"start_time": "2026-09-15T10:02:00.005000000Z",
			"end_time": "2026-09-15T10:02:00.017000000Z", "status": "UNSET",
			"attributes": {
				"db.system.name": "microsoft.sql_server",
				"mssql.connection.attempt": 1,
				"mssql.connection.endpoint_role": "database"
			},
			"events": []
		},
		{
			"trace_id": "33333333333333333333333333333333",
			"span_id": "3333333333333306", "parent_span_id": "3333333333333303",
			"name": "mssql.driver.connection.prelogin", "kind": "INTERNAL",
			"start_time": "2026-09-15T10:02:00.017000000Z",
			"end_time": "2026-09-15T10:02:00.025000000Z", "status": "UNSET",
			"attributes": {
				"db.system.name": "microsoft.sql_server",
				"mssql.connection.attempt": 1,
				"mssql.connection.endpoint_role": "database"
			},
			"events": []
		},
		{
			"trace_id": "33333333333333333333333333333333",
			"span_id": "3333333333333307", "parent_span_id": "3333333333333303",
			"name": "mssql.driver.connection.tls", "kind": "INTERNAL",
			"start_time": "2026-09-15T10:02:00.025000000Z",
			"end_time": "2026-09-15T10:02:00.055000000Z", "status": "UNSET",
			"attributes": {
				"db.system.name": "microsoft.sql_server",
				"mssql.connection.attempt": 1,
				"mssql.connection.endpoint_role": "database"
			},
			"events": []
		},
		{
			"trace_id": "33333333333333333333333333333333",
			"span_id": "3333333333333308", "parent_span_id": "3333333333333303",
			"name": "mssql.driver.connection.login", "kind": "INTERNAL",
			"start_time": "2026-09-15T10:02:00.055000000Z",
			"end_time": "2026-09-15T10:02:00.110000000Z", "status": "UNSET",
			"attributes": {
				"db.system.name": "microsoft.sql_server",
				"mssql.connection.attempt": 1,
				"mssql.authentication.method": "sql_password"
			},
			"events": []
		},
		{
			"trace_id": "33333333333333333333333333333333",
			"span_id": "3333333333333309", "parent_span_id": "3333333333333301",
			"name": "mssql.driver.connection.initialize", "kind": "INTERNAL",
			"start_time": "2026-09-15T10:02:00.110000000Z",
			"end_time": "2026-09-15T10:02:00.120000000Z", "status": "UNSET",
			"attributes": { "db.system.name": "microsoft.sql_server" },
			"events": []
		}
	]
}
```

The logical GUID and user-agent appear only on the root; the TDS ClientConnectionId appears only on the attempt. Initialization is a child of open, not login: login acceptance alone is insufficient to declare the overall connection successful. If initialization fails, that child and the root become `ERROR` while the completed login remains `UNSET`.

#### Additional cases at a glance

| Observed failure | Failing child / event | Root outcome and category | Evidence shipped / absent |
|---|---|---|---|
| Certificate hostname validation fails | TLS + `mssql.driver.error` | `failure` / `tls_security` | Code `jdbc:R_certNameFailed` when captured at validation, actual internal driver code 5 when available. No outer-wrapper duplicate, certificate names, hostname, or certificate dump. |
| Managed identity request expires | Token acquisition + error and timeout events | `timeout` / `timeout` | Authentication method `managed_identity`, timeout kind `token_request`, actual budget if known, `exception.type` from the observed failure origin. Do not mark `authentication` merely because the outer wrapper mentions a token. Failure counter +1; timeout counter +1. |
| First endpoint redirects; redirected TCP connect is refused | First attempt, redirect event, second attempt/socket failure | `failure` / `network_connectivity` | `redirect_count=1`, `attempt_count=2`, `endpoint_role=redirect_target`, observed exception class on the failure event. The redirect event establishes the observed redirect. No target hostname/IP. |
| Invalid port rejected before transport | Configuration + error event | `failure` / `configuration` | Code `jdbc:R_invalidPortNumber`, `attempt_count=0`; no unexecuted transport children or TDS UUID. Retain native IDs and root GUID; omit raw invalid value. |
| JVM cannot allocate required connection resources | Actual owning phase, if safely recordable | `failure` / `client_resource_exhaustion` | Actual OOM/resource evidence only; do not infer a driver leak. Export may be impossible under exhaustion. |
| Application pool waits with all connections checked out | Pool-owned acquisition span, not a fabricated driver open | Pool outcome / `client_resource_exhaustion` when capacity is proved | Source `external_pool`; no driver failure counter increment if the driver was never invoked. Pool timeout alone may reflect a different underlying connection failure. |

#### Acceptance checklist

- Test serialized schemas/privacy, applicable child parenting/timing, all 16 canonical categories, and single-event placement for root-origin versus child-origin failures. Parse both complete worked-example envelopes and assert 4/8 spans, valid parent references, and contained span/event timestamps.
- Assert no `mssql.connection.phase`, reachability, or process-local connection ID in either worked JSON envelope; GUID, terminal `mssql.connection.failure_phase`, and user-agent are root-only. DNS has no TDS UUID; SQL rejection has exactly one, on the attempt. Preserve each native `trace_id`/`span_id` and every nonroot `parent_span_id`, without custom trace-ID attributes or implicit inheritance; verify root/attempt lookups and `mssql.error.phase` on the single failing-child event.
- Verify physical-lifetime GUID retention even for unexported successful opens, root-only placement on later separately rooted operations, optional root-only driver-ID diagnostics, no default TDS UUID on reset or reset-dispatch events, and failure-phase placement only on the terminal failed owning operation root, never a nested reset/phase span.
- Exercise pre-ACK DNS/TCP failures, SQL rejection versus token failure, retries/redirects, and separate pooled/deferred-reset attribution; preserve original SQL behavior and exceptions.
- Verify attribute-free terminal counters independently of retained-span volume; test bounded recording/queues, sampling restrictions, exporter failures, and best-effort behavior under severe resource exhaustion.