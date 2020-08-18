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
import com.azure.core.util.CoreUtils;
import com.azure.core.util.logging.ClientLogger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;


final class KeyVaultHttpPipelineBuilder {
    private final ClientLogger logger = new ClientLogger(KeyVaultHttpPipelineBuilder.class);
    // This is properties file's name.
    private static final String AZURE_KEY_VAULT_SECRETS = "azure-key-vault-secrets.properties";
    private static final String SDK_NAME = "name";
    private static final String SDK_VERSION = "version";

    private final List<HttpPipelinePolicy> policies;
    final Map<String, String> properties;
    private KeyVaultCredential credential;
    private HttpLogOptions httpLogOptions;
    private final RetryPolicy retryPolicy;

    /**
     * The constructor with defaults.
     */
    public KeyVaultHttpPipelineBuilder() {
        retryPolicy = new RetryPolicy();
        httpLogOptions = new HttpLogOptions();
        policies = new ArrayList<>();
        properties = CoreUtils.getProperties(AZURE_KEY_VAULT_SECRETS);
    }

    public HttpPipeline buildPipeline() {
        Configuration buildConfiguration = Configuration.getGlobalConfiguration().clone();

        if (credential == null) {
            throw logger.logExceptionAsError(
                    new IllegalStateException(
                            "Token Credential should be specified."));
        }

        // Closest to API goes first, closest to wire goes last.
        final List<HttpPipelinePolicy> policies = new ArrayList<>();

        String clientName = properties.getOrDefault(SDK_NAME, "UnknownName");
        String clientVersion = properties.getOrDefault(SDK_VERSION, "UnknownVersion");
        policies.add(new UserAgentPolicy(httpLogOptions.getApplicationId(), clientName, clientVersion,
                buildConfiguration));
        HttpPolicyProviders.addBeforeRetryPolicies(policies);
        policies.add(retryPolicy);
        policies.add(new KeyVaultCustomCredentialPolicy(credential));
        policies.addAll(this.policies);
        HttpPolicyProviders.addAfterRetryPolicies(policies);
        policies.add(new HttpLoggingPolicy(httpLogOptions));

        HttpPipeline pipeline = new HttpPipelineBuilder()
                                        .policies(policies.toArray(new HttpPipelinePolicy[0]))
                                        .build();

        return pipeline;
    }


    /**
     * Sets the credential to use when authenticating HTTP requests.
     *
     * @param credential The credential to use for authenticating HTTP requests.
     * @return the updated KVHttpPipelineBuilder object.
     * @throws NullPointerException if {@code credential} is {@code null}.
     */
    public KeyVaultHttpPipelineBuilder credential(KeyVaultCredential credential) {
        Objects.requireNonNull(credential);
        this.credential = credential;
        return this;
    }
}

