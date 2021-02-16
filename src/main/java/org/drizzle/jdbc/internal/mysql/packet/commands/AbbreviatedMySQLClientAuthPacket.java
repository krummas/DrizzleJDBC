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
import java.util.Arrays;
import java.util.Set;

import org.drizzle.jdbc.internal.common.packet.CommandPacket;
import org.drizzle.jdbc.internal.common.packet.buffer.WriteBuffer;
import org.drizzle.jdbc.internal.mysql.MySQLServerCapabilities;

/**
 * used for starting ssl connections!
 * 
 * 
 */
public class AbbreviatedMySQLClientAuthPacket implements CommandPacket {
    private final WriteBuffer writeBuffer;

    /**
     * 
     * Creates a new <code>AbbreviatedMySQLClientAuthPacket</code> object, 
     * sent by the client to initiate SSL handshake
     * 
     * MySQL Payload
     * 4              capability flags, CLIENT_SSL always set
     * 4              max-packet size
     * 1              character set
     * string[23]     reserved (all [0])
     * 
     * For MariaDB :
     * 4              capability flags, CLIENT_SSL always set
     * 4              max-packet size
     * 1              character set
     * string<19> reserved
     * if not (server_capabilities & CLIENT_MYSQL)
     *      int<4> extended client capabilities
     * else
     *      string<4> reserved
     * 
     * @param serverCapabilities
     */
    public AbbreviatedMySQLClientAuthPacket(final Set<MySQLServerCapabilities> serverCapabilities) {
        writeBuffer = new WriteBuffer();
        writeBuffer.writeInt(MySQLServerCapabilities.fromSet(serverCapabilities));
        writeBuffer.writeInt(1024*1024*1024); /* max allowed packet : 1GB */
        writeBuffer.writeByte((byte) 33);
        byte[] filler = new byte[23];
        Arrays.fill(filler, (byte) 0x00);
        writeBuffer.writeByteArray(filler); /* filler */
    }


    public int send(final OutputStream os) throws IOException {
        os.write(writeBuffer.getLengthWithPacketSeq((byte) 1));
        os.write(writeBuffer.getBuffer(),0,writeBuffer.getLength());
        os.flush();
        return 1;
    }
}