/*
 * Drizzle-JDBC
 *
 * Copyright (c) 2009-2019, Marcus Eriksson, Stephane Giron, Marc Isambart, Trond Norbye
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
package org.drizzle.jdbc.internal.mysql;

import org.drizzle.jdbc.internal.common.QueryException;
import org.drizzle.jdbc.internal.mysql.packet.MySQLAuthSwitchRequest;

public class AuthPluginFactory {

    /**
     * Get the plugin to use when an AuthSwitchRequest was received, i.e. initial
     * password method was different from what the server expected
     */
    public static AuthPlugin getAuthPlugin(MySQLAuthSwitchRequest authSwitchPacket) throws QueryException {
        if (authSwitchPacket.useOldAuth())
            return new OldAuthPlugin(authSwitchPacket);
        else if ("sha256_password".equals(authSwitchPacket.getPluginName()))
            return new Sha256AuthPlugin(authSwitchPacket);
        else if ("caching_sha2_password".equals(authSwitchPacket.getPluginName()))
            return new CachingSha2AuthPlugin(authSwitchPacket);
        else if ("mysql_native_password".equals(authSwitchPacket.getPluginName()))
            return new NativeAuthPlugin(authSwitchPacket);

        throw new QueryException("Unknown authentication method " + authSwitchPacket.getPluginName());
    }

    /**
     * Get the plugin to use for initial authentication exchange. By default, it
     * will use native authentication except if the server default authentication
     * protocol is sha256 or caching_sha2.
     */
    public static AuthPlugin getAuthPlugin(String authPluginName, byte[] seed) throws QueryException {
        if ("sha256_password".equals(authPluginName))
            return new Sha256AuthPlugin(seed);
        else if ("caching_sha2_password".equals(authPluginName))
            return new CachingSha2AuthPlugin(seed);
        else
            return new NativeAuthPlugin(seed);
    }

}
