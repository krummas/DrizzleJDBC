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

package org.drizzle.jdbc.internal.mysql.packet.commands;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Set;

import org.drizzle.jdbc.internal.common.QueryException;
import org.drizzle.jdbc.internal.common.packet.CommandPacket;
import org.drizzle.jdbc.internal.common.packet.buffer.WriteBuffer;
import org.drizzle.jdbc.internal.mysql.AuthPlugin;
import org.drizzle.jdbc.internal.mysql.AuthPluginFactory;
import org.drizzle.jdbc.internal.mysql.MySQLServerCapabilities;

/**
 * 4                            client_flags 4                            max_packet_size 1 charset_number 23 (filler)
 * always 0x00... n (Null-Terminated String)   user n (Length Coded Binary)      scramble_buff (1 + x bytes) 1 (filler)
 * always 0x00 n (Null-Terminated String) databasename
 * <p/>
 * client_flags:            CLIENT_xxx options. The list of possible flag values is in the description of the Handshake
 * Initialisation Packet, for server_capabilities. For some of the bits, the server passed "what it's capable of". The
 * client leaves some of the bits on, adds others, and passes back to the server. One important flag is: whether
 * compression is desired.
 * <p/>
 * max_packet_size:         the maximum number of bytes in a packet for the client
 * <p/>
 * charset_number:          in the same domain as the server_language field that the server passes in the Handshake
 * Initialization packet.
 * <p/>
 * user:                    identification
 * <p/>
 * scramble_buff:           the password, after encrypting using the scramble_buff contents passed by the server (see
 * "Password functions" section elsewhere in this document) if length is zero, no password was given
 * <p/>
 * databasename:            name of schema to use initially
 * <p/>
 * User: marcuse Date: Jan 16, 2009 Time: 11:19:31 AM
 */
public class MySQLClientAuthPacket implements CommandPacket {
    private final WriteBuffer writeBuffer;
    private final byte packetSeq;

    public MySQLClientAuthPacket(final String username, final String password, final String database,
            final Set<MySQLServerCapabilities> serverCapabilities, final byte[] seed, byte packetSeq,
            String defaultAuthPlugin, boolean useSSL) throws QueryException {
        this.packetSeq = packetSeq;
        writeBuffer = new WriteBuffer();
        final byte[] scrambledPassword;

        AuthPlugin authPlugin = AuthPluginFactory.getAuthPlugin(defaultAuthPlugin, seed);
        scrambledPassword = authPlugin.getEncodedPassword(password, useSSL);

        final byte serverLanguage = 33;
        writeBuffer.writeInt(MySQLServerCapabilities.fromSet(serverCapabilities)).
                writeInt(0xffffff).
                writeByte(serverLanguage). // 1
                writeBytes((byte) 0, 23). // 23
                writeString(username). // strlen username
                writeByte((byte) 0). // 1
                writeByte((byte) scrambledPassword.length).
                writeByteArray(scrambledPassword);

        if (serverCapabilities.contains(MySQLServerCapabilities.CONNECT_WITH_DB)) {
            writeBuffer.writeString(database).writeByte((byte) 0);
        }
        
        if(serverCapabilities.contains(MySQLServerCapabilities.CLIENT_PLUGIN_AUTH))
        {
            writeBuffer.writeString(authPlugin.getDefaultPlugin());
            writeBuffer.writeByte((byte) 0);
        }
    }

    public int send(final OutputStream os) throws IOException {
        os.write(writeBuffer.getLengthWithPacketSeq(packetSeq));
        os.write(writeBuffer.getBuffer(), 0, writeBuffer.getLength());
        os.flush();
        return 1;
    }
}