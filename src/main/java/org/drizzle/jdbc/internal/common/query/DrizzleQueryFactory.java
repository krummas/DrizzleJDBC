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
                         
package org.drizzle.jdbc.internal.common.query;

import java.util.concurrent.ConcurrentHashMap;

/**
 * . User: marcuse Date: Mar 18, 2009 Time: 10:14:27 PM
 */
public class DrizzleQueryFactory implements QueryFactory {
    private static final ConcurrentHashMap<String, ParameterizedQuery> PREPARED_CACHE = new ConcurrentHashMap<String, ParameterizedQuery>();
    public Query createQuery(final String query) {
        return new DrizzleQuery(query);
    }
    
    public DrizzleQuery createQuery(byte[] query)
    {
        return new DrizzleQuery(query);
    }
    public ParameterizedQuery createParameterizedQuery(final String query, boolean noCache) {

        if(noCache)
            return new DrizzleParameterizedQuery(query);

        // Cache prepared statements
        ParameterizedQuery pq = DrizzleQueryFactory.PREPARED_CACHE.get(query);

        if(pq == null) {
            pq = new DrizzleParameterizedQuery(query);
            DrizzleQueryFactory.PREPARED_CACHE.put(query, pq);
            return pq;
        } else {
            return new DrizzleParameterizedQuery(pq);
        }
    }

    public ParameterizedQuery createParameterizedQuery(final ParameterizedQuery dQuery) {
        return new DrizzleParameterizedQuery(dQuery);
    }
}
