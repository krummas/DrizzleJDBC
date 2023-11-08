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
import java.io.OutputStream;
import java.security.NoSuchAlgorithmException;

import org.drizzle.jdbc.internal.common.QueryException;
import org.drizzle.jdbc.internal.common.Utils;
import org.drizzle.jdbc.internal.common.packet.RawPacket;
import org.drizzle.jdbc.internal.common.packet.buffer.WriteBuffer;
import org.drizzle.jdbc.internal.mysql.packet.MySQLAuthSwitchRequest;

public class NativeAuthPlugin implements AuthPlugin {

    private final byte[] seed;
    private int packetSeq;

    public NativeAuthPlugin(MySQLAuthSwitchRequest authSwitchPacket) {
        this.seed = authSwitchPacket.getAuthPluginData();
        this.packetSeq = authSwitchPacket.getPacketSeq() + 1;
    }

    public NativeAuthPlugin(byte[] seed) {
        this.seed = seed;
    }

    public RawPacket authenticate(MySQLProtocol protocol) throws IOException, QueryException {
        // TODO Auto-generated method stub
        WriteBuffer writeBuffer = new WriteBuffer();
        final byte[] scrambledPassword;
        OutputStream os = protocol.getWriter();

        try {
            scrambledPassword = Utils.encryptPassword(protocol.getPassword(), seed);
            writeBuffer.writeByteArray(scrambledPassword);

            os.write(writeBuffer.getLengthWithPacketSeq((byte) (packetSeq)));
            os.write(writeBuffer.getBuffer(), 0, writeBuffer.getLength());
            os.flush();

        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Could not use SHA-1, failing", e);
        }

        return protocol.getPacketFetcher().getRawPacket();
    }

    public byte[] getEncodedPassword(String password, boolean ssl) {
        try {
            return Utils.encryptPassword(password, seed);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Could not use SHA-1, failing", e);
        }
    }

    public String getDefaultPlugin() {
        return "mysql_native_password";
    }

    public RawPacket readAuthMoreData(RawPacket rp, MySQLProtocol protocol)throws IOException, QueryException {
        throw new QueryException("Don't know how to read auth more data in native auth plugin!");
    }
}
