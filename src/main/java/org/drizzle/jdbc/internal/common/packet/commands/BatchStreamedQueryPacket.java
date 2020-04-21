/*
 * Drizzle-JDBC
 *
 * Copyright (c) 2009-2020, Marcus Eriksson, Stephane Giron
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

package org.drizzle.jdbc.internal.common.packet.commands;

import static org.drizzle.jdbc.internal.common.packet.buffer.WriteBuffer.intToByteArray;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.drizzle.jdbc.internal.common.QueryException;
import org.drizzle.jdbc.internal.common.Utils;
import org.drizzle.jdbc.internal.common.packet.CommandPacket;
import org.drizzle.jdbc.internal.common.query.Query;

public class BatchStreamedQueryPacket implements CommandPacket
{

    // Maximum packet length coded on 3 bytes
    private static final int MAX_PACKET_LENGTH = 0x00FFFFFF;
    
    private final static Logger log = Logger
                                            .getLogger(BatchStreamedQueryPacket.class
                                                    .getName());


    private final List<Query> queries;
    private final long size;
    
    public BatchStreamedQueryPacket(List<Query> batchList,
            long batchSize)
    {
        this.queries = batchList;
        this.size = batchSize;

    }

    public int send(final OutputStream ostream) throws IOException, QueryException {
        long length = size;
                
        if (length > MAX_PACKET_LENGTH)
        {
            // This is going to be splitted accross several network packets
            return sendSplittedQuery(ostream, length);
        }
        else
        {
            byte[] byteHeader = Utils.copyWithLength(intToByteArray( (int)length + 1), 5);
            byteHeader[3] = (byte) 0;
            byteHeader[4] = (byte) 0x03;
            ostream.write(byteHeader);
      
            for (Query query : queries)
            {
                query.writeTo(ostream);
                ostream.write((byte)';');
            }
            ostream.flush();
            return 0;
        }        
    }

    private int sendSplittedQuery(OutputStream ostream, long length) throws QueryException,
            IOException
    {
        if(log.isLoggable(Level.FINEST)) {
            log.finest("sending splitted query");
        }
        long totalRemainingBytes = size;
        int queryRemainingBytes = 0, queryRemainingOffset = 0;
        
        int packetIndex = 0;
        boolean missingSemicol = false;
        
        byte[] byteHeader = null;
        int packLength = (int) Math.min(totalRemainingBytes, MAX_PACKET_LENGTH),
                fullPacketLength = 0;
        
        int queryNum = 1;
        
        while(totalRemainingBytes > 0 || fullPacketLength == MAX_PACKET_LENGTH)
        {
            packLength = (int)Math.min(totalRemainingBytes, MAX_PACKET_LENGTH);
            fullPacketLength = packLength;
            
            if(log.isLoggable(Level.FINEST)) {
                log.finest("Sending packet " + packetIndex + " - size = " + packLength);
            }
            
            if (packetIndex == 0)
            {
                // Send the first packet
                byteHeader = Utils.copyWithLength(intToByteArray(packLength), 5);
                // Add the command byte
                byteHeader[4] = (byte) 0x03;
                // And remove 1 byte from available data length
                packLength -= 1;
                byteHeader[3] = (byte) packetIndex;
                ostream.write(byteHeader);
            }
            else
            {
                byteHeader = Utils.copyWithLength(intToByteArray(packLength), 4);
                byteHeader[3] = (byte) packetIndex;
                ostream.write(byteHeader);
            }
             
            while (packLength > 0)
            {
                if (missingSemicol)
                {
                    ostream.write((byte) ';');
                    missingSemicol = false;
                    totalRemainingBytes--;
                    packLength--;
                }
                else if (queryRemainingBytes > 0)
                {
                    Query query = queries.get(0);
                    if (queryRemainingBytes <= packLength)
                    {
                        query.writeTo(ostream, queryRemainingOffset,
                                queryRemainingBytes);

                        if (queryRemainingBytes == packLength)
                        {
                            missingSemicol = true;
                        }
                        else
                        {
                            ostream.write((byte) ';');
                            packLength--;
                            totalRemainingBytes--;
                        }
                        totalRemainingBytes -= queryRemainingBytes;
                        packLength -= queryRemainingBytes;
                        queryRemainingBytes = 0;
                        queryRemainingOffset = 0;
                        queries.remove(0);
                    }
                    else
                    {
                        query.writeTo(ostream, queryRemainingOffset,
                                packLength);
                        queryRemainingBytes -= packLength;
                        totalRemainingBytes -= packLength;
                        queryRemainingOffset += packLength;
                        packLength = 0;
                    }
                }
                else
                {
                    Query query = queries.get(0);
                    if (query.length() <= packLength)
                    {
                        query.writeTo(ostream);
                        queries.remove(0);
                        
                        packLength -= query.length();
                        totalRemainingBytes -= query.length();
                        if (packLength > 0)
                        {
                            ostream.write((byte) ';');
                            packLength--;
                            totalRemainingBytes--;
                        }
                        else
                        {
                            missingSemicol = true;
                        }
                    }
                    else
                    {
                        query.writeTo(ostream, 0, packLength);
                        totalRemainingBytes -= packLength;
                        queryRemainingBytes = query.length() - packLength;
                        queryRemainingOffset = packLength;
                        packLength = 0;
                    }
                }
            }
            ostream.flush();
            packetIndex++;
        }
        
        return packetIndex;
    }    
}