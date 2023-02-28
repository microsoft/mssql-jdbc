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

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;


/**
 * The HTTP pipeline builder which includes all the necessary HTTP pipeline policies that will be applied for
 * sending and receiving HTTP requests to the Key Vault service.
 */
final class KeyVaultHttpPipelineBuilder {

    private final List<HttpPipelinePolicy> policies;
    private KeyVaultTokenCredential credential;
    private HttpLogOptions httpLogOptions;
    private final RetryPolicy retryPolicy;

    /**
     * The constructor with default retry policy and log options.
     */
    KeyVaultHttpPipelineBuilder() {
        retryPolicy = new RetryPolicy();
        httpLogOptions = new HttpLogOptions();
        policies = new ArrayList<>();
    }

    /**
     * Builds the HTTP pipeline with all the necessary HTTP policies included in the pipeline.
     *
     * @return A fully built HTTP pipeline including the default HTTP client.
     * @throws SQLServerException
     *         If the {@link KeyVaultCustomCredentialPolicy} policy cannot be added to the pipeline.
     */
    HttpPipeline buildPipeline() throws SQLServerException {
        // Closest to API goes first, closest to wire goes last.
        final List<HttpPipelinePolicy> pol = new ArrayList<>();

        HttpPolicyProviders.addBeforeRetryPolicies(pol);
        pol.add(retryPolicy);
        pol.add(new KeyVaultCustomCredentialPolicy(credential));
        pol.addAll(this.policies);
        HttpPolicyProviders.addAfterRetryPolicies(pol);
        pol.add(new HttpLoggingPolicy(httpLogOptions));

        return new HttpPipelineBuilder().policies(pol.toArray(new HttpPipelinePolicy[0])).build();
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
