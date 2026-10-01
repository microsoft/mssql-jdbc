/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.StringReader;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.helpers.DefaultHandler;


final class SQLServerQueryNotificationParser {
    private SQLServerQueryNotificationParser() {}

    static SQLServerQueryNotification parse(String payload) throws SQLServerException {
        if (null == payload || payload.isEmpty()) {
            throwInvalidPayload(null);
        }

        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            factory.setXIncludeAware(false);
            factory.setExpandEntityReferences(false);

            javax.xml.parsers.DocumentBuilder builder = factory.newDocumentBuilder();
            builder.setErrorHandler(new DefaultHandler());
            Document document = builder.parse(new InputSource(new StringReader(payload)));
            Element root = document.getDocumentElement();
            if (null == root || !"QueryNotification".equals(root.getLocalName())) {
                throwInvalidPayload(null);
            }

            NodeList messages = root.getElementsByTagNameNS("*", "Message");
            if (1 != messages.getLength()) {
                throwInvalidPayload(null);
            }

            return new SQLServerQueryNotification(messages.item(0).getTextContent(), root.getAttribute("type"),
                    root.getAttribute("source"), root.getAttribute("info"), parseLong(root, "id"),
                    (int) parseLong(root, "database_id"));
        } catch (SQLServerException e) {
            throw e;
        } catch (Exception e) {
            throwInvalidPayload(e);
            return null;
        }
    }

    private static long parseLong(Element element, String attribute) throws SQLServerException {
        try {
            return Long.parseLong(element.getAttribute(attribute));
        } catch (NumberFormatException e) {
            throwInvalidPayload(e);
            return 0;
        }
    }

    private static void throwInvalidPayload(Throwable cause) throws SQLServerException {
        SQLServerException.makeFromDriverError(null, null,
                SQLServerException.getErrString("R_invalidQueryNotificationPayload"), null, false, cause);
    }
}
