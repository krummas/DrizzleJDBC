package org.drizzle.jdbc;

import org.junit.Test;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.Connection;
import java.util.Properties;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Jun 13, 2009
 * Time: 1:29:33 PM
 * To change this template use File | Settings | File Templates.
 */
public class MySQLDriverTest extends DriverTest {
    private Connection connection;
    public MySQLDriverTest() throws SQLException {
        connection=ConnectionCheck.Get_ConnectionMySQL();
       // connection = DriverManager.getConnection("jdbc:mysql://10.100.100.50:3306/test_units_jdbc");
    }
    @Override
    public Connection getConnection() {
        return connection;
    }
    
    @Test
    public void testAuthConnection() throws SQLException {
        Connection conn=ConnectionCheck.Get_ConnectionMySQL_WithUsername("test:test");
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select * from t1");
        rs.close();
        stmt.close();
        conn.close();
    }

    @Test
    public void testAuthConnection2() throws SQLException {
        Connection conn=ConnectionCheck.Get_ConnectionMySQL_WithUsername("e_passwd:");
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select * from t1");
        rs.close();
        stmt.close();
        conn.close();
    }

    @Test
    public void testAuthConnectionProperties() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user","test");
        props.setProperty("password","test");

        Connection conn;
        if (ConnectionCheck.mysql_host.contains(":"))
        {
            conn= DriverManager.getConnection("jdbc:mysql:thin://teest:teest@"+ConnectionCheck.mysql_host+"/test_units_jdbc",props);
        }
        else
        {
            conn= DriverManager.getConnection("jdbc:mysql:thin://teest:teest@"+ConnectionCheck.mysql_host+":3306/test_units_jdbc",props);
        }
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select * from t1");
        rs.close();
        stmt.close();
        conn.close();
    }
    @Test
    public void testBit() throws SQLException {
        Connection connection = getConnection();
        connection.createStatement().execute("drop table if exists bittest");
        connection.createStatement().execute("create table bittest (a bit(1), b bit(3))");
        connection.createStatement().execute("insert into bittest values (null, null), (0, 0), (1, 1), (0, 2), (1, 3);");
        ResultSet rs = connection.createStatement().executeQuery("select * from bittest");
        while(rs.next()) {
            if(rs.getObject(1) != null)
                System.out.println(rs.getObject(1).getClass());
            System.out.println(rs.getByte(1));
        }
    }
    @Test
    public void testSmallint() throws SQLException {
        Connection connection = getConnection();
        connection.createStatement().execute("drop table if exists smallinttest");
        connection.createStatement().execute("create table smallinttest (i1 smallint, i2 smallint unsigned)");
        connection.createStatement().execute("insert into smallinttest values (null, null), (0, 0), (-1, 1), (-32768, 32767), (32767, 65535)");
        ResultSet rs = connection.createStatement().executeQuery("select * from smallinttest");
        while(rs.next()) {
            
            System.out.println(rs.getObject(2));
            if(rs.getObject(2) != null)
                System.out.println(rs.getObject(2).getClass());
            else System.out.println("---");
        }
    }
    @Test
    public void testMediumint() throws SQLException {
        Connection connection = getConnection();
        connection.createStatement().execute("drop table if exists mediuminttest");
        connection.createStatement().execute("create table mediuminttest (i1 mediumint, i2 mediumint unsigned)");
        connection.createStatement().execute("insert into mediuminttest values (null, null), (0, 0), (-1, 1), (-8388608, 8388607), (8388607, 16777215)");
        ResultSet rs = connection.createStatement().executeQuery("select * from mediuminttest");
        while(rs.next()) {

            System.out.println(rs.getObject(2));
            if(rs.getObject(2) != null)
                System.out.println(rs.getObject(2).getClass());
            else System.out.println("---");
        }
    }
    @Test
    public void testTimestamp() throws SQLException {
        Connection connection = getConnection();
        connection.createStatement().execute("drop table if exists t");
        connection.createStatement().execute("create table t (t timestamp)");
        connection.createStatement().execute("insert into t values  ('1971-01-01 01:01:01'), ('2007-12-03 15:50:18'), ('2037-12-31 23:59:59')");
        ResultSet rs = connection.createStatement().executeQuery("select * from t");
        while(rs.next()) {
            System.out.println("---");
            System.out.println("ee "+rs.getTimestamp(1) );
         //   if(rs.getObject(1) != null)
           //     System.out.println("uu "+rs.getObject(1).getClass());
            //else System.out.println("xxx");
        }
    }
    @Test
    public void testDatetime() throws SQLException {
        Connection connection = getConnection();
        connection.createStatement().execute("drop table if exists t");
        connection.createStatement().execute("create table t (t datetime)");
        connection.createStatement().execute("insert into t values (null), ('1000-01-01 00:00:00'), ('2007-12-03 15:47:32'), ('9999-12-31 23:59:59')");
        ResultSet rs = connection.createStatement().executeQuery("select * from t");
        while(rs.next()) {
            System.out.println("---");
            System.out.println("ee "+rs.getObject(1));
         //   if(rs.getObject(1) != null)
           //     System.out.println("uu "+rs.getObject(1).getClass());
            //else System.out.println("xxx");
        }
    }
    @Test
    public void testFloat() throws SQLException {
        Connection connection = getConnection();
        connection.createStatement().execute("drop table if exists t");
        connection.createStatement().execute("create table t (f float)");
        connection.createStatement().execute("insert into t values (null), (-3.402823466E+38), (-1.175494351E-38), (0), (1.175494351E-38), (3.402823466E+38)");
        ResultSet rs = connection.createStatement().executeQuery("select * from t");
        while(rs.next()) {

            System.out.println(rs.getObject(1));
            if(rs.getObject(1) != null)
                System.out.println(rs.getObject(1).getClass());
            else System.out.println("---");
        }
    }

    @Test
    public void testDouble() throws SQLException {
        Connection connection = getConnection();
        connection.createStatement().execute("drop table if exists t");
        connection.createStatement().execute("create table t (d double)");
        connection.createStatement().execute("insert into t values (null), (-1.7976931348623157E+308), (-2.2250738585072014E-308), (0), (2.2250738585072014E-308), (1.7976931348623157E+308)");
        ResultSet rs = connection.createStatement().executeQuery("select * from t");
        while(rs.next()) {

            System.out.println(rs.getObject(1));
            if(rs.getObject(1) != null)
                System.out.println(rs.getObject(1).getClass());
            else System.out.println("---");
        }
    }
     @Test
    public void bigintTest() throws SQLException {
        getConnection().createStatement().execute("drop table if exists biginttest");
        getConnection().createStatement().execute(
                        "create table biginttest (i1 bigint, i2 bigint unsigned)");
        getConnection().createStatement().execute("insert into biginttest values (null, null), (0, 0), (-1, 1), (-9223372036854775808, 9223372036854775807), (9223372036854775807, 18446744073709551615)");
        ResultSet rs = getConnection().createStatement().executeQuery("select * from biginttest");
        assertTrue(rs.next());
        assertEquals(null, rs.getObject(1));
        assertEquals(null, rs.getObject(2));
        assertTrue(rs.next());
        assertEquals(BigInteger.ZERO, rs.getObject(1));
        assertEquals(BigInteger.ZERO, rs.getObject(2));
        assertTrue(rs.next());
        assertEquals(new BigInteger("-1"), rs.getObject(1));
        assertEquals(BigInteger.ONE, rs.getObject(2));
        assertTrue(rs.next());
        assertEquals(new BigInteger("-9223372036854775808"), rs.getObject(1));
        assertEquals(new BigInteger("9223372036854775807"), rs.getObject(2));
        assertTrue(rs.next());
        assertEquals(new BigInteger("9223372036854775807"), rs.getObject(1));
        assertEquals(new BigInteger("18446744073709551615"), rs.getObject(2));
        assertFalse(rs.next());
    }
 
}
