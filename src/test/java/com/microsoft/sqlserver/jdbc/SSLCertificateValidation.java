/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;


@RunWith(JUnitPlatform.class)
public class SSLCertificateValidation {

    /**
     * Tests our internal method, validateServerName() against different possible names in SSL certificate.
     * 
     * @throws Exception
     */
    @Test
    public void testValidateServerName() throws Exception {

        String serverName = "msjdbc.database.windows.net";
        String serverName2 = "bbbbuuzzuzzzzzz.example.net";
        String serverName3 = "xn--ms.database.windows.net";

        // Set up the HostNameOverrideX509TrustManager object using reflection
        TDSChannel tdsc = new TDSChannel(new SQLServerConnection("someConnectionProperty"));
        Class<?> hsoClass = Class.forName("com.microsoft.sqlserver.jdbc.TDSChannel$HostNameOverrideX509TrustManager");
        Constructor<?> constructor = hsoClass.getDeclaredConstructors()[0];
        constructor.setAccessible(true);
        Object hsoObject = constructor.newInstance(null, tdsc, null, serverName);
        Method method = hsoObject.getClass().getDeclaredMethod("validateServerName", String.class);
        method.setAccessible(true);

        /*
         * Server Name = msjdbc.database.windows.net SAN = msjdbc.database.windows.net Expected result: true
         */
        assertTrue((boolean) method.invoke(hsoObject, "msjdbc.database.windows.net"));

        /*
         * Server Name = msjdbc.database.windows.net SAN = msjdbc***.database.windows.net Expected result: true
         */
        assertTrue((boolean) method.invoke(hsoObject, "msjdbc***.database.windows.net"));

        /*
         * Server Name = msjdbc.database.windows.net SAN = ms*bc.database.windows.net Expected result: true
         */
        assertTrue((boolean) method.invoke(hsoObject, "ms*bc.database.windows.net"));

        /*
         * Server Name = msjdbc.database.windows.net SAN = *bc.database.windows.net Expected result: true
         */
        assertTrue((boolean) method.invoke(hsoObject, "*bc.database.windows.net"));

        /*
         * Server Name = msjdbc.database.windows.net SAN = ms*.database.windows.net Expected result: true
         */
        assertTrue((boolean) method.invoke(hsoObject, "ms*.database.windows.net"));

        /*
         * Server Name = msjdbc.database.windows.net SAN = *jd*.database.windows.net Expected result: true
         */
        assertTrue((boolean) method.invoke(hsoObject, "*jd*.database.windows.net"));

        /*
         * Server Name = msjdbc.database.windows.net SAN = ms.*.net Expected result: false
         */
        assertFalse((boolean) method.invoke(hsoObject, "ms.*.net"));

        /*
         * Server Name = msjdbc.database.windows.net SAN = msjdbc.asd*dsa.windows.net Expected result: false
         */
        assertFalse((boolean) method.invoke(hsoObject, "msjdbc.asd*dsa.windows.net"));

        /*
         * Server Name = msjdbc.database.windows.net SAN = *.*.windows.net Expected result: false
         */
        assertFalse((boolean) method.invoke(hsoObject, ".*.windows.net"));

        /*
         * Server Name = msjdbc.database.windows.net SAN = msjdbc.*.windows.net Expected result: false
         */
        assertFalse((boolean) method.invoke(hsoObject, "msjdbc.*.windows.net"));

        /*
         * Server Name = msjdbc.database.windows.net SAN = *.*.windows.net Expected result: false Note: multiple
         * wildcards are not allowed, so this case shouldn't happen, but we still make sure to fail this.
         */
        assertFalse((boolean) method.invoke(hsoObject, "*.*.windows.net"));

        /*
         * Server Name = msjdbc.database.windows.net SAN = *.com Expected result: false A cert with * plus a top-level
         * domain is not allowed.
         */
        assertFalse((boolean) method.invoke(hsoObject, "*.com"));

        /*
         * Server Name = msjdbc.database.windows.net SAN = xn--ms*.database.windows.net Expected result: false
         */
        assertFalse((boolean) method.invoke(hsoObject, "xn--ms*.database.windows.net"));

        /*
         * Server Name = msjdbc.database.windows.net SAN = * Expected result: false
         */
        assertFalse((boolean) method.invoke(hsoObject, "*"));

        /*
         * Server Name = msjdbc.database.windows.net SAN = ms*atabase.windows.net Expected result: false
         */
        assertFalse((boolean) method.invoke(hsoObject, "ms*atabase.windows.net"));

        hsoObject = constructor.newInstance(null, tdsc, null, serverName2);
        method = hsoObject.getClass().getDeclaredMethod("validateServerName", String.class);
        method.setAccessible(true);

        /*
         * Server Name = bbbbuuzzuzzzzzz.example.net SAN = b*zzz.example.net Expected result: true
         */
        assertTrue((boolean) method.invoke(hsoObject, "b*zzz.example.net"));

        hsoObject = constructor.newInstance(null, tdsc, null, serverName3);
        method = hsoObject.getClass().getDeclaredMethod("validateServerName", String.class);
        method.setAccessible(true);

        /*
         * Server Name = xn--ms.database.windows.net SAN = xn--ms.database.windows.net Expected result: true
         */
        assertTrue((boolean) method.invoke(hsoObject, "xn--ms.database.windows.net"));
    }
}
