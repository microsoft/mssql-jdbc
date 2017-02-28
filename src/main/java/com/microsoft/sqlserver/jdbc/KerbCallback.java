package com.microsoft.sqlserver.jdbc;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;

public class KerbCallback implements CallbackHandler {

    private final SQLServerConnection con;

    KerbCallback(SQLServerConnection con) {
        this.con = con;
    }

    private static String getAnyOf(Callback callback, Properties properties, String... names)
            throws UnsupportedCallbackException {
        for (String name : names) {
            String val = properties.getProperty(name);
            if (val != null && !val.trim().isEmpty()) {
                return val;
            }
        }
        throw new UnsupportedCallbackException(callback,
                "Cannot get any of properties: " + Arrays.toString(names) + " from con properties");
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (int i = 0; i < callbacks.length; i++) {
            Callback callback = callbacks[i];
            if (callback instanceof NameCallback) {
                ((NameCallback) callback).setName(getAnyOf(callback, con.activeConnectionProperties,
                        "user", SQLServerDriverStringProperty.USER.name()));
            } else if (callback instanceof PasswordCallback) {
                String password = getAnyOf(callback, con.activeConnectionProperties,
                        "password", SQLServerDriverStringProperty.PASSWORD.name());
                ((PasswordCallback) callbacks[i])
                        .setPassword(password.toCharArray());

            } else {
                throw new UnsupportedCallbackException(callback, "Unrecognized Callback type: " + callback.getClass());
            }
        }

    }

}
