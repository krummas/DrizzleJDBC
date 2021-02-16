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
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import org.drizzle.jdbc.internal.common.QueryException;
import org.drizzle.jdbc.internal.common.Utils;
import org.drizzle.jdbc.internal.common.packet.RawPacket;
import org.drizzle.jdbc.internal.common.packet.buffer.WriteBuffer;
import org.drizzle.jdbc.internal.mysql.packet.MySQLAuthSwitchRequest;

public class Sha256AuthPlugin implements AuthPlugin {

    protected static final byte RETRIEVE_RSA_PUBLIC_KEY_CODE = 0x01;
    protected final byte[] seed;
    protected int packetSeq;

    private static final String BEGIN_PUBLIC_KEY = "-----BEGIN PUBLIC KEY-----";
    private static final String END_PUBLIC_KEY = "-----END PUBLIC KEY-----";

    public Sha256AuthPlugin(MySQLAuthSwitchRequest authSwitchPacket) {
        seed = authSwitchPacket.getAuthPluginData();
        packetSeq = authSwitchPacket.getPacketSeq() + 1;
    }

    public Sha256AuthPlugin(byte[] seed) {
        this.seed = seed;
    }

    public RawPacket authenticate(MySQLProtocol protocol) throws IOException, QueryException {
        WriteBuffer writeBuffer = new WriteBuffer();
        OutputStream os = protocol.getWriter();

        if (protocol.useSsl()) {
            // With SSL : null terminated password in clear text
            writeBuffer.writeByteArray(protocol.getPassword().getBytes());
            writeBuffer.writeByte((byte) 0x00);

            os.write(writeBuffer.getLengthWithPacketSeq((byte) packetSeq));
            os.write(writeBuffer.getBuffer(), 0, writeBuffer.getLength());
            os.flush();
        } else {
            authenticateWithRSAPublicKey(protocol, RETRIEVE_RSA_PUBLIC_KEY_CODE);
        }
        return protocol.getPacketFetcher().getRawPacket();
    }

    public byte[] getEncodedPassword(String password, boolean ssl) {
        if (ssl) {
            final byte[] returnBytes = new byte[password.getBytes().length + 1];
            System.arraycopy(password.getBytes(), 0, returnBytes, 0, password.getBytes().length);
            returnBytes[password.getBytes().length] = 0x00;
            return returnBytes;
        } else
            try {
                return Utils.encryptPassword(password, seed);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("Could not use SHA-1, failing", e);
            }
    }

    /**
     * Even if server default authentication protocol is sha256, we send the password with native mysql authentication.
     * It does not seem that the public key exchange is implemented for initial authentication protocol. 
     */
    public String getDefaultPlugin() {
        return "mysql_native_password";
    }

    protected void authenticateWithRSAPublicKey(MySQLProtocol protocol, byte code) throws IOException {
        WriteBuffer writeBuffer;
        OutputStream os = protocol.getWriter();

        // Full auth path (password not found in server cache)
        // We need to fetch the server RSA public key
        String keyString = getServerRSAPublicKey(protocol, code);
        PublicKey pkey = getPublicKeyFromString(keyString);

        Cipher rsaCipher;

        byte[] scrambledPassword;

        try {
            rsaCipher = Cipher.getInstance("RSA/ECB/OAEPPadding");
        } catch (NoSuchAlgorithmException e1) {
            throw new RuntimeException("Unable to use RSA/ECB/OAEPPadding", e1);
        } catch (NoSuchPaddingException e1) {
            throw new RuntimeException("Unable to use RSA/ECB/OAEPPadding", e1);
        }

        try {
            rsaCipher.init(Cipher.ENCRYPT_MODE, pkey);
        } catch (InvalidKeyException e) {
            throw new RuntimeException("Invalid key", e);
        }

        scrambledPassword = xOrPassword(protocol.getPassword(), seed);

        byte[] encrypted;
        try {
            encrypted = rsaCipher.doFinal(scrambledPassword);
        } catch (IllegalBlockSizeException e) {
            throw new RuntimeException("Unable to encrypt with RSA", e);
        } catch (BadPaddingException e) {
            throw new RuntimeException("Unable to encrypt with RSA", e);
        }
        writeBuffer = new WriteBuffer();
        writeBuffer.writeByteArray(encrypted);

        os.write(writeBuffer.getLengthWithPacketSeq((byte) (packetSeq)));
        os.write(writeBuffer.getBuffer(), 0, writeBuffer.getLength());
        os.flush();

    }

    private String getServerRSAPublicKey(MySQLProtocol protocol, byte code) throws IOException {
        // Either the server RSA public key was provided with serverPublicKey parameter
        // or we need to fetch it from the server
        WriteBuffer writeBuffer = new WriteBuffer();
        OutputStream os = protocol.getWriter();

        String keyString = null;
        if (protocol.getMysqlPublicKey() != null) {
            // Reading key from file
            keyString = protocol.getMysqlPublicKey();
        } else {
            // Fetching from server => send 0x01 to the server that will send the key back
            writeBuffer.writeByte((byte) code);
            os.write(writeBuffer.getLengthWithPacketSeq((byte) packetSeq));
            os.write(writeBuffer.getBuffer(), 0, writeBuffer.getLength());
            os.flush();

            RawPacket rp = protocol.getPacketFetcher().getRawPacket();
            packetSeq = rp.getPacketSeq() + 1;
            keyString = new String(rp.getByteBuffer().array());
        }

        keyString = keyString.replaceAll("\n", "").trim();

        if (keyString.startsWith(BEGIN_PUBLIC_KEY)) {
            int endPos = keyString.indexOf(END_PUBLIC_KEY, BEGIN_PUBLIC_KEY.length());
            if (endPos == -1)
                throw new RuntimeException("Bad public key format : " + keyString);
            keyString = keyString.substring(BEGIN_PUBLIC_KEY.length(), endPos);
        } else
            throw new RuntimeException("Bad public key format : " + keyString);
        return keyString;
    }

    private PublicKey getPublicKeyFromString(String keyString) {
        X509EncodedKeySpec keySpec = new X509EncodedKeySpec(Base64.getDecoder().decode(keyString.getBytes()));

        PublicKey pkey = null;
        try {
            pkey = KeyFactory.getInstance("RSA").generatePublic(keySpec);
        } catch (InvalidKeySpecException e1) {
            throw new RuntimeException("Bad public key spec : " +  keyString, e1);
        } catch (NoSuchAlgorithmException e1) {
            throw new RuntimeException("Missing RSA algorithm", e1);
        }
        return pkey;
    }
    
    /**
     * Processes a password for sha256 password authentication
     * <p/>
     * Send an xor of the output of the password and the seed
     *
     * @param password
     *            the password to encrypt
     * @param seed
     *            the seed to use
     * @return a scrambled password
     */
    private byte[] xOrPassword(final String password, final byte[] seed)
    {
        if (password == null || password.equals("")) {
            return new byte[0];
        }

        final byte[] returnBytes = new byte[password.getBytes().length + 1];
        System.arraycopy(password.getBytes(), 0, returnBytes, 0, password.getBytes().length);
        returnBytes[returnBytes.length - 1] = (byte) 0x00;
        for (int i = 0; i < returnBytes.length; i++) {
            returnBytes[i] ^= seed[i % seed.length];
        }
        return returnBytes;
    }

    public RawPacket readAuthMoreData(RawPacket rp, MySQLProtocol protocol)throws IOException, QueryException {
        throw new QueryException("Don't know how to read auth more data in sha256 auth plugin!");
    }
}
