package com.microsoft.sqlserver.jdbc;

import java.net.MalformedURLException;
import java.text.MessageFormat;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationException;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.sqlserver.jdbc.SQLServerConnection.ActiveDirectoryAuthentication;
import com.microsoft.sqlserver.jdbc.SQLServerConnection.SqlFedAuthInfo;

class SQLServerADAL4JUtils {

    static SqlFedAuthToken getSqlFedAuthToken(SqlFedAuthInfo fedAuthInfo,
                                          String user,
                                          String password,
                                          String authenticationString) throws SQLServerException {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        try {
            AuthenticationContext context = new AuthenticationContext(fedAuthInfo.stsurl, false, executorService);
            Future<AuthenticationResult> future = context.acquireToken(fedAuthInfo.spn, ActiveDirectoryAuthentication.JDBC_FEDAUTH_CLIENT_ID, user,
                    password, null);

            AuthenticationResult authenticationResult = future.get();
            SqlFedAuthToken fedAuthToken = new SqlFedAuthToken(authenticationResult.getAccessToken(), authenticationResult.getExpiresOnDate());

            return fedAuthToken;
        }
        catch (MalformedURLException | InterruptedException e) {
            throw new SQLServerException(e.getMessage(), e);
        }
        catch (ExecutionException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_ADALExecution"));
            Object[] msgArgs = {user, authenticationString};

            // the cause error message uses \\n\\r which does not give correct format
            // change it to \r\n to provide correct format
            String correctedErrorMessage = e.getCause().getMessage().replaceAll("\\\\r\\\\n", "\r\n");
            AuthenticationException correctedAuthenticationException = new AuthenticationException(correctedErrorMessage);

            // SQLServerException is caused by ExecutionException, which is caused by AuthenticationException
            // to match the exception tree before error message correction
            ExecutionException correctedExecutionException = new ExecutionException(correctedAuthenticationException);

            throw new SQLServerException(form.format(msgArgs), null, 0, correctedExecutionException);
        }
        finally {
            executorService.shutdown();
        }
    }
}
