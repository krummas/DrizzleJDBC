/*
 * Drizzle-JDBC
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following
 * conditions are met:
 *
 *  Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 *  Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided with the distribution.
 *  Neither the name of the driver nor the names of its contributors may be used to endorse or promote products derived
 *   from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,
 * BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.drizzle.jdbc;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses a jdbc url.
 * <p/>
 * User: marcuse Date: Apr 21, 2009 Time: 9:32:34 AM
 */
public class JDBCUrl {
    private final DBType dbType;
    private final String username;
    private final String password;
    private final String hostname;
    private final int port;
    private final String database;

    public enum DBType {
        DRIZZLE, MYSQL
    }

    private JDBCUrl(DBType dbType, String username, String password, String hostname, int port, String database) {
        this.dbType = dbType;
        this.username = username;
        this.password = password;
        this.hostname = hostname;
        this.port = port;
        this.database = database;
    }

    public static JDBCUrl parse(String url) {
        DBType dbType;
        if (url.startsWith("jdbc:mysql:thin://")) {
            dbType = DBType.MYSQL;
            url = url.substring("jdbc:mysql:thin://".length());
        } else if (url.startsWith("jdbc:drizzle://")) {
            dbType = DBType.DRIZZLE;
            url = url.substring("jdbc:drizzle://".length());
        } else {
            return null;
        }

        String hostname;
        int port = (dbType == DBType.DRIZZLE) ? 3306 : 3306;
        String username = "";
        String password = "";
        String database;

        int atSignIndex = url.indexOf("@");
        if (atSignIndex != -1) {
            String userPassCombo = url.substring(0, atSignIndex);
            url = url.substring(atSignIndex+1);

            int userPassDividerIndex = userPassCombo.indexOf(":");
            if (userPassDividerIndex == -1) {
                username = userPassCombo;
            } else {
                username = userPassCombo.substring(0, userPassDividerIndex);
                password = userPassCombo.substring(userPassDividerIndex+1);
            }
        }
        int slashIndex = url.indexOf("/");
        String hostPortCombo = url.substring(0, slashIndex);
        url = url.substring(slashIndex + 1);
        int ipv6StartIndex = hostPortCombo.indexOf("[");
        int ipv6EndIndex = hostPortCombo.indexOf("]");
        if (ipv6StartIndex >= 0 && ipv6EndIndex > ipv6StartIndex) {
        	hostname = hostPortCombo.substring(ipv6StartIndex + 1, ipv6EndIndex);
        	int hostPortDividerIndex = hostPortCombo.indexOf(":", ipv6EndIndex + 1);
        	if (hostPortDividerIndex != -1) {
        		port = Integer.parseInt(hostPortCombo.substring(hostPortDividerIndex + 1));
        	}
        } else {
        	int hostPortDividerIndex = hostPortCombo.indexOf(":");
        	if (hostPortDividerIndex == -1) {
        		hostname = hostPortCombo;
        	} else {
        		hostname = hostPortCombo.substring(0, hostPortDividerIndex);
        		port = Integer.parseInt(hostPortCombo.substring(hostPortDividerIndex + 1));
        	}
        }
        slashIndex = url.indexOf("/");
        if (slashIndex == -1) {
        	database = url;
        } else {
        	database = url.substring(0, slashIndex);
        }
        return new JDBCUrl(dbType, username, password, hostname, port, database);
   }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    public String getDatabase() {
        return database;
    }

    public DBType getDBType() {
        return this.dbType;
    }

}