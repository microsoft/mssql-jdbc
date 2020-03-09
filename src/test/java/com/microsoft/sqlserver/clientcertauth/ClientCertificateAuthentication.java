package com.microsoft.sqlserver.clientcertauth;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
@Tag(Constants.clientCertAuth)
public class ClientCertificateAuthentication extends AbstractTest {

    @Test
    public void CERTest() throws Exception {
    }

}
