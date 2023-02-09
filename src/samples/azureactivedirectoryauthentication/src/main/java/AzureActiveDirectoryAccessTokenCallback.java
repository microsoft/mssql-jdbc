/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package azureactivedirectoryauthentication.src.main.java;

import com.microsoft.aad.msal4j.IClientCredential;
import com.microsoft.aad.msal4j.ClientCredentialFactory;
import com.microsoft.aad.msal4j.ConfidentialClientApplication;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import com.microsoft.aad.msal4j.ClientCredentialParameters;
import java.sql.Connection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Sample code that demonstrates how to use access token callback.
 */
public class AzureActiveDirectoryAccessTokenCallback {

    public static void main(String[] args) {

        SQLServerAccessTokenCallback callback = new SQLServerAccessTokenCallback() {
            @Override
            public SqlAuthenticationToken getAccessToken(String stsurl, String spn) {

                String clientSecret = "<your-client-secret>";
                String clientId = "<your-client-id>";

                String scope = spn + "/.default";
                Set<String> scopes = new HashSet<>();
                scopes.add(scope);

                try {
                    ExecutorService executorService = Executors.newSingleThreadExecutor();
                    IClientCredential credential = ClientCredentialFactory.createFromSecret(clientSecret);
                    ConfidentialClientApplication clientApplication = ConfidentialClientApplication
                            .builder(clientId, credential).executorService(executorService).authority(stsurl).build();
                    CompletableFuture<IAuthenticationResult> future = clientApplication
                            .acquireToken(ClientCredentialParameters.builder(scopes).build());

                    IAuthenticationResult authenticationResult = future.get();
                    String accessToken = authenticationResult.accessToken();

                    return new SqlAuthenticationToken(accessToken, authenticationResult.expiresOnDate().getTime());
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }
        };

        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setServerName("<your-server-name>");
        ds.setDatabaseName("<your-database-name>");
        ds.setAccessTokenCallback(callback);

        try (Connection conn = (SQLServerConnection) ds.getConnection()) {
            System.out.println("Connected...");
        }

    }
}
