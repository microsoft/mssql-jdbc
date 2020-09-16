/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import com.azure.core.http.HttpPipeline;
import com.azure.core.http.HttpPipelineBuilder;
import com.azure.core.http.policy.HttpLogOptions;
import com.azure.core.http.policy.HttpLoggingPolicy;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.core.http.policy.HttpPolicyProviders;
import com.azure.core.http.policy.RetryPolicy;
import com.azure.core.http.policy.UserAgentPolicy;
import com.azure.core.util.Configuration;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;


final class KeyVaultHttpPipelineBuilder {
    private static final String APPLICATION_ID = "mssql-jdbc";
    private static final String SDK_NAME = "azure-security-keyvault-keys";
    private static final String SDK_VERSION = "4.2.0";

    private final List<HttpPipelinePolicy> policies;
    private KeyVaultTokenCredential credential;
    private HttpLogOptions httpLogOptions;
    private final RetryPolicy retryPolicy;

    /**
     * The constructor with defaults.
     */
    KeyVaultHttpPipelineBuilder() {
        retryPolicy = new RetryPolicy();
        httpLogOptions = new HttpLogOptions();
        policies = new ArrayList<>();
    }

    HttpPipeline buildPipeline() throws SQLServerException {
        Configuration buildConfiguration = Configuration.getGlobalConfiguration().clone();

        // Closest to API goes first, closest to wire goes last.
        final List<HttpPipelinePolicy> policies = new ArrayList<>();

        policies.add(new UserAgentPolicy(APPLICATION_ID, SDK_NAME, SDK_VERSION, buildConfiguration));
        HttpPolicyProviders.addBeforeRetryPolicies(policies);
        policies.add(retryPolicy);
        policies.add(new KeyVaultCustomCredentialPolicy(credential));
        policies.addAll(this.policies);
        HttpPolicyProviders.addAfterRetryPolicies(policies);
        policies.add(new HttpLoggingPolicy(httpLogOptions));

        return new HttpPipelineBuilder().policies(policies.toArray(new HttpPipelinePolicy[0])).build();
    }

    /**
     * Sets the credential to use when authenticating HTTP requests.
     *
     * @param credential
     *        The credential to use for authenticating HTTP requests.
     * @return the updated KVHttpPipelineBuilder object.
     * @throws SQLServerException
     */
    KeyVaultHttpPipelineBuilder credential(KeyVaultTokenCredential credential) throws SQLServerException {
        if (null == credential) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_NullValue"));
            Object[] msgArgs1 = {"Credential"};
            throw new SQLServerException(form.format(msgArgs1), null);
        }

        this.credential = credential;
        return this;
    }
}
