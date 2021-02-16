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

public class CachingSha2AuthPlugin extends Sha256AuthPlugin {
    protected static final byte RETRIEVE_RSA_PUBLIC_KEY_CODE = 0x02;

    public CachingSha2AuthPlugin(MySQLAuthSwitchRequest authSwitchPacket) {
        super(authSwitchPacket);
    }

    public CachingSha2AuthPlugin(byte[] seed) {
        super(seed);
    }

    public RawPacket authenticate(MySQLProtocol protocol) throws IOException, QueryException {
        WriteBuffer writeBuffer = new WriteBuffer();
        OutputStream os = protocol.getWriter();

        byte[] scrambledPassword;
        try {
            // Send the encrypted password
            scrambledPassword = Utils.encryptPasswordSha256(protocol.getPassword(), seed);
            writeBuffer.writeByteArray(scrambledPassword);

            os.write(writeBuffer.getLengthWithPacketSeq((byte) packetSeq));
            os.write(writeBuffer.getBuffer(), 0, writeBuffer.getLength());
            os.flush();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Unable to use SHA-256", e);
        }
        RawPacket packet = protocol.getPacketFetcher().getRawPacket();
        if ((packet.getByteBuffer().get(0) & 0xFF) == 0x01) {
            return readAuthMoreData(packet, protocol);
        }
        return protocol.getPacketFetcher().getRawPacket();
    }

    public byte[] getEncodedPassword(String password, boolean ssl) {
        try {
            return Utils.encryptPasswordSha256(password, seed);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Could not use SHA-256, failing", e);
        }
    }

    public String getDefaultPlugin() {
        return "caching_sha2_password";
    }

    public RawPacket readAuthMoreData(RawPacket packet, MySQLProtocol protocol) throws IOException, QueryException {
        WriteBuffer writeBuffer = new WriteBuffer();
        OutputStream os = protocol.getWriter();
        if (!protocol.useSsl()) {
            packetSeq = packet.getPacketSeq() + 1;
        }
        if ((packet.getByteBuffer().get(1) & 0xFF) == 0x03) {
            // FAST auth path ok : nothing more to do
        } else if ((packet.getByteBuffer().get(1) & 0xFF) == 0x04) {
            if (protocol.useSsl()) {
                // SSL + full auth : send password in clear text
                writeBuffer = new WriteBuffer();
                writeBuffer.writeByteArray(protocol.getPassword().getBytes());
                writeBuffer.writeByte((byte) 0x00);

                os.write(writeBuffer.getLengthWithPacketSeq((byte) (packet.getPacketSeq() + 1)));
                os.write(writeBuffer.getBuffer(), 0, writeBuffer.getLength());
                os.flush();
            }
            else {
                authenticateWithRSAPublicKey(protocol, RETRIEVE_RSA_PUBLIC_KEY_CODE);
            }
        }
        return protocol.getPacketFetcher().getRawPacket();
    }
}
