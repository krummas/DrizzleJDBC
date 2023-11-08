/*
 * Drizzle-JDBC
 *
 * Copyright (c) 2009-2011, Marcus Eriksson, Marc Isambart, Stephane Giron
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

package org.drizzle.jdbc.internal.common.query;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

/**
 * . User: marcuse Date: Feb 20, 2009 Time: 10:43:58 PM
 */
public class DrizzleQuery implements Query {

    private String query;
    private final byte[] queryToSend;

    public DrizzleQuery(final String query) {
        this.query = query;
        try {
            queryToSend = query.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Unsupported encoding: " + e.getMessage(), e);
        }
    }

    public DrizzleQuery(final byte[] query) {
        queryToSend = query;
    }

    public int length() {
        return queryToSend.length;
    }

    public void writeTo(final OutputStream os) throws IOException {
        os.write(queryToSend, 0, queryToSend.length);
    }

    public String getQuery() {
        if(query == null)
            try {
                this.query = new String(queryToSend, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("Unsupported encoding: " + e.getMessage(), e);
            }
        return query;
    }

    public QueryType getQueryType() {
        return QueryType.classifyQuery(getQuery());
    }

    @Override
    public boolean equals(final Object otherObj) {
        return otherObj instanceof DrizzleQuery && (((DrizzleQuery) otherObj).getQuery()).equals(getQuery());
    }

    public void writeTo(OutputStream ostream, int offset, int packLength) throws IOException
    {
        ostream.write(queryToSend, offset, packLength);
    }

    @Override
    public String toString()
    {
        return getQuery();
    }

    @Override
    public byte[] getBytes()
    {
        return queryToSend;
    }
}
