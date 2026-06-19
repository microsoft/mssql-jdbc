/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.otel;

import java.lang.reflect.Method;
import java.util.Properties;

/**
 * Verifies end-to-end that {@code otelAccessTokenCallbackClass} =
 * {@link AzureCliAccessTokenCallback} produces a real Azure AD JWT in the OTLP/HTTP
 * {@code Authorization: Bearer ...} header that the driver attaches to every export.
 *
 * <p>It drives the driver's actual (package-private) {@code OtelBootstrap.otlpHeaders} code path
 * with the real callback, so a green run proves the live token-acquisition + header wiring.
 * Requires the {@code az} CLI on PATH and a logged-in {@code ~/.azure} (the otel-poc app
 * container provides both). Exits non-zero on failure.
 *
 * <pre>
 * docker compose run --rm --no-deps app \
 *   mvn -B -Pjre11 -DskipTests test-compile \
 *   org.codehaus.mojo:exec-maven-plugin:3.1.0:java -Dexec.classpathScope=test \
 *   -Dexec.mainClass=com.microsoft.sqlserver.jdbc.otel.OtelTokenHeaderCheck
 * </pre>
 */
public final class OtelTokenHeaderCheck {

    private OtelTokenHeaderCheck() {}

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.setProperty("otelEndpoint", "http://localhost:4318/v1/metrics");
        props.setProperty("otelAccessTokenCallbackClass", AzureCliAccessTokenCallback.class.getName());

        Class<?> bootstrap = Class.forName("com.microsoft.sqlserver.jdbc.OtelBootstrap");
        Class<?> tokenClass = Class.forName("com.microsoft.sqlserver.jdbc.SqlAuthenticationToken");
        Method otlpHeaders = bootstrap.getDeclaredMethod("otlpHeaders", Properties.class, tokenClass);
        otlpHeaders.setAccessible(true);

        String[][] headers = (String[][]) otlpHeaders.invoke(null, props, null);

        String authorization = null;
        for (String[] kv : headers) {
            if ("Authorization".equals(kv[0])) {
                authorization = kv[1];
                break;
            }
        }
        if (authorization == null) {
            throw new IllegalStateException("FAIL: OtelBootstrap.otlpHeaders produced no Authorization header.");
        }

        String token = authorization.regionMatches(true, 0, "Bearer ", 0, "Bearer ".length())
                ? authorization.substring("Bearer ".length())
                : authorization;
        String[] segments = token.split("\\.");
        boolean looksLikeJwt = token.startsWith("eyJ") && segments.length == 3;

        System.out.println("Authorization: Bearer " + redact(token));
        System.out.println("JWT segments=" + segments.length + " starts-with-eyJ=" + token.startsWith("eyJ"));

        if (!looksLikeJwt) {
            throw new IllegalStateException(
                    "FAIL: Authorization is not a JWT (expected eyJ... with 3 dot-separated segments).");
        }
        System.out.println("SUCCESS: OTLP exports will carry a real AAD JWT minted by AzureCliCredential.");
    }

    private static String redact(String token) {
        if (token.length() <= 12) {
            return "<" + token.length() + " chars>";
        }
        return token.substring(0, 6) + "..." + token.substring(token.length() - 4) + " (" + token.length() + " chars)";
    }
}
