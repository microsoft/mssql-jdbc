/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.connection;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.util.ArrayList;
import java.util.List;

import javax.sql.XAConnection;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerXADataSource;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
@Tag(Constants.XA)
public class XADataSourceTest extends AbstractTest {
    private static String connectionUrlSSL = connectionString + "encrypt=true;trustServerCertificate=false;";
    private static List<String> certificates = new ArrayList<>();

    @Test
    public void testPKCS12() throws Exception {
        SQLServerXADataSource ds = new SQLServerXADataSource();

        // populate certificates arraylist with certificates
        // Only re-populate the truststore if need arises in the future.

        // populateCertificates();
        // String trustStore = (new TrustStore(certificates)).getFileName();

        String trustStore = System.getProperty("pkcs12_truststore");
        String url = connectionUrlSSL + "trustStore=" + trustStore + ";";
        ds.setURL(url);
        ds.setTrustStorePassword(System.getProperty("pkcs12_truststore_password"));
        XAConnection connection = ds.getXAConnection();
        connection.close();
    }

    private static void populateCertificates() {
        certificates.add("sql-2k8r2-sp3-1.galaxy.ad.cer");
        certificates.add("sql-2k8-sp4-1.galaxy.ad.cer");
        certificates.add("sql-2k12-sp3-2.galaxy.ad.cer");
        certificates.add("sql-2k14-2.galaxy.ad.cer");
        certificates.add("sql-2k16-01.galaxy.ad.cer");
        certificates.add("sql-2k16-02.galaxy.ad.cer");
        certificates.add("sql-2k16-04.galaxy.ad.cer");
        certificates.add("sql-2k17-01.galaxy.ad.cer");
        certificates.add("sql-2k17-03.galaxy.ad.cer");
        certificates.add("sql-2k17-04.galaxy.ad.cer");
        certificates.add("sql-2k19-01.galaxy.ad.cer");
        certificates.add("sql-2k19-02.galaxy.ad.cer");
    }

    static class TrustStore {
        private File trustStoreFile;

        static final String TRUST_STORE_PWD = "<your_password_here>";

        TrustStore(List<String> certificateNames) throws Exception {
            trustStoreFile = File.createTempFile("myTrustStore", null, new File("."));
            // trustStoreFile.deleteOnExit();
            KeyStore ks = KeyStore.getInstance("PKCS12");
            ks.load(null, null);

            for (String certificateName : certificateNames) {
                ks.setCertificateEntry(certificateName, getCertificate(certificateName));
            }

            FileOutputStream os = new FileOutputStream(trustStoreFile);
            ks.store(os, TRUST_STORE_PWD.toCharArray());
            os.flush();
            os.close();
        }

        final String getFileName() throws Exception {
            return trustStoreFile.getCanonicalPath();
        }

        private static java.security.cert.Certificate getCertificate(String certname) throws Exception {
            FileInputStream is = new FileInputStream(certname);
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            return cf.generateCertificate(is);
        }
    }
}
