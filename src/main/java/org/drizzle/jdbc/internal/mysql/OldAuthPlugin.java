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

import java.io.IOException;

import org.drizzle.jdbc.internal.common.QueryException;
import org.drizzle.jdbc.internal.common.Utils;
import org.drizzle.jdbc.internal.common.packet.RawPacket;
import org.drizzle.jdbc.internal.mysql.packet.MySQLAuthSwitchRequest;
import org.drizzle.jdbc.internal.mysql.packet.commands.MySQLClientOldPasswordAuthPacket;

public class OldAuthPlugin implements AuthPlugin {

    
    private MySQLAuthSwitchRequest authSwitchRequest;

    public OldAuthPlugin(MySQLAuthSwitchRequest authSwitchPacket) {
        this.authSwitchRequest = authSwitchPacket;
    }

    public RawPacket authenticate(MySQLProtocol protocol) throws IOException, QueryException {
        
        final MySQLClientOldPasswordAuthPacket oldPassPacket = new MySQLClientOldPasswordAuthPacket(
                protocol.getPassword(), Utils.copyWithLength(authSwitchRequest.getGreetingPacket().getSeed(), 8), authSwitchRequest.getPacketSeq() + 1);
        oldPassPacket.send(protocol.getWriter());
        return protocol.getPacketFetcher().getRawPacket();
    }

    public byte[] getEncodedPassword(String password, boolean ssl) {
        return null;
    }
    
    public String getDefaultPlugin() {
        return "mysql_native_password";
    }
}
