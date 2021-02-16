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
package org.drizzle.jdbc.internal.mysql.packet;

import java.io.IOException;

import org.drizzle.jdbc.internal.common.packet.RawPacket;
import org.drizzle.jdbc.internal.common.packet.buffer.Reader;
import org.drizzle.jdbc.internal.mysql.MySQLServerCapabilities;

public class MySQLAuthSwitchRequest {

    private final boolean useOldAuth;
    private final String pluginName;
    private byte[] authPluginData;
    private final MySQLGreetingReadPacket greetingPacket;
    private final int packetSeq;

    public MySQLAuthSwitchRequest(RawPacket rawPacket, MySQLGreetingReadPacket greetingPacket) throws IOException {
        final Reader reader = new Reader(rawPacket);

        reader.readByte();

        // Server asking for another authentication
        if (reader.getRemainingSize() == 0) {
            useOldAuth = true;
            pluginName = null;
        } else {
            useOldAuth = false;
            if (greetingPacket.getServerCapabilities().contains(MySQLServerCapabilities.CLIENT_PLUGIN_AUTH)) {
                pluginName = reader.readString("ASCII");
            } else
                pluginName = null;
            authPluginData = reader.readRawBytes(reader.getRemainingSize() - 1);
        }
        this.greetingPacket = greetingPacket;
        this.packetSeq = rawPacket.getPacketSeq();
    }

    /**
     * Returns the useOldAuth value.
     * 
     * @return Returns the useOldAuth.
     */
    public boolean useOldAuth() {
        return useOldAuth;
    }

    /**
     * Returns the pluginName value.
     * 
     * @return Returns the pluginName.
     */
    public String getPluginName() {
        return pluginName;
    }

    /**
     * Returns the authPluginData value.
     * 
     * @return Returns the authPluginData.
     */
    public byte[] getAuthPluginData() {
        return authPluginData;
    }

    public MySQLGreetingReadPacket getGreetingPacket() {
        return greetingPacket;
    }

    public int getPacketSeq() {
        return packetSeq;
    }
}