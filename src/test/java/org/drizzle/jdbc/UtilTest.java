package org.drizzle.jdbc;

import org.drizzle.jdbc.internal.common.Utils;
import org.junit.Test;
import static org.drizzle.jdbc.internal.common.Utils.countChars;
import static junit.framework.Assert.assertEquals;

/**
 * User: marcuse
 * Date: Feb 19, 2009
 * Time: 9:19:24 PM
 */
public class UtilTest {
    @Test
    public void testCountChars() {
        String test = "aaa?bbcc??xx?";
        assertEquals(4,countChars(test,'?'));
    }


    @Test
    public void testArrayToInt() {
        for(long i = Integer.MAX_VALUE - 100; i <= Integer.MAX_VALUE; i++) {
            int x = Utils.byteArrayToInt(Long.valueOf(i).toString().getBytes());
            assertEquals(i, x);
        }
        for(long i = Integer.MAX_VALUE; i <= (long)Integer.MAX_VALUE+100; i++) {
            int x = Utils.byteArrayToInt(Long.valueOf(i).toString().getBytes());
            assertEquals(Integer.MAX_VALUE, x);
        }
        for(long i = (long)Integer.MIN_VALUE - 100; i <= Integer.MIN_VALUE; i++) {
            int x = Utils.byteArrayToInt(Long.valueOf(i).toString().getBytes());
            assertEquals(Integer.MIN_VALUE, x);
        }
        for(long i = (long)Integer.MIN_VALUE; i <= (long)Integer.MIN_VALUE+100; i++) {
            int x = Utils.byteArrayToInt(Long.valueOf(i).toString().getBytes());
            assertEquals(i, x);
        }
    }
    @Test
    public void testArrayToLong() {
        for(long i = Long.MAX_VALUE - 100; i < Long.MAX_VALUE; i++) {
            long x = Utils.byteArrayToLong(Long.valueOf(i).toString().getBytes());
            assertEquals(i, x);
        }
        for(long i = Long.MIN_VALUE; i <= Long.MIN_VALUE+100; i++) {
            long x = Utils.byteArrayToLong(Long.valueOf(i).toString().getBytes());
            assertEquals(i, x);
        }
    }
    @Test
    public void testArrayToShort() {
        for(short i = Short.MAX_VALUE - 100; i <= Short.MAX_VALUE-1; i++) {
            short x = Utils.byteArrayToShort(Short.valueOf(i).toString().getBytes());
            assertEquals(i, x);
        }
        for(short i = Short.MIN_VALUE; i <= Short.MIN_VALUE+100; i++) {
            short x = Utils.byteArrayToShort(Short.valueOf(i).toString().getBytes());
            assertEquals(i, x);
        }
    }
    @Test
    public void testArrayToByte() {
        for(byte i = Byte.MAX_VALUE - 100; i <= Byte.MAX_VALUE-1; i++) {
            byte x = Utils.byteArrayToByte(Byte.valueOf(i).toString().getBytes());
            assertEquals(i, x);
        }
        for(byte i = Byte.MIN_VALUE; i <= Byte.MIN_VALUE+100; i++) {
            byte x = Utils.byteArrayToByte(Byte.valueOf(i).toString().getBytes());
            assertEquals(i, x);
        }
    }
    @Test
    public void testOverflow1() {
        assertEquals(Long.MAX_VALUE, Utils.byteArrayToLong("9223372036854775809".getBytes()));
    }
    @Test
    public void testOverflow2() {
        assertEquals(Integer.MAX_VALUE, Utils.byteArrayToInt("9223372036854775809".getBytes()));
    }
    @Test
    public void testOverflow3() {
        assertEquals(Short.MAX_VALUE, Utils.byteArrayToShort("9223372036854775809".getBytes()));
    }
    @Test
    public void testOverflow4() {
        assertEquals(Byte.MAX_VALUE, Utils.byteArrayToByte("9223372036854775809".getBytes()));
    }
    @Test
    public void testUnderflow1() {
        assertEquals(Long.MIN_VALUE, Utils.byteArrayToLong("-9223372036854775809".getBytes()));
    }
    @Test
    public void testUnderflow2() {
        assertEquals(Integer.MIN_VALUE, Utils.byteArrayToInt("-9223372036854775809".getBytes()));
    }
    @Test
    public void testUnderflow3() {
        assertEquals(Short.MIN_VALUE, Utils.byteArrayToShort("-9223372036854775809".getBytes()));
    }
    @Test
    public void testUnderflow4() {
        assertEquals(Byte.MIN_VALUE, Utils.byteArrayToByte("-9223372036854775809".getBytes()));
    }
    @Test(expected = NumberFormatException.class)
    public void testBad1() {
        Utils.byteArrayToLong("--".getBytes());
    }

    @Test(expected = NumberFormatException.class)
    public void testBad2() {
        Utils.byteArrayToInt("123a".getBytes());
    }
    @Test(expected = NumberFormatException.class)
    public void testBad3() {
        Utils.byteArrayToInt("-b123".getBytes());
    }
    @Test(expected = NumberFormatException.class)
    public void testBad4() {
        Utils.byteArrayToByte("-".getBytes());
    }
    @Test(expected = NumberFormatException.class)
    public void testBad5() {
        Utils.byteArrayToShort("123a".getBytes());
    }
    @Test(expected = NumberFormatException.class)
    public void testBad6() {
        Utils.byteArrayToShort("-123x".getBytes());
    }
}