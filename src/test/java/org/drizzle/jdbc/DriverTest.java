package org.drizzle.jdbc;

import org.junit.Test;
import org.junit.Before;
import org.drizzle.jdbc.packet.buffer.WriteBuffer;
import org.apache.log4j.BasicConfigurator;

import java.sql.*;
import java.util.List;

import static junit.framework.Assert.assertEquals;

/**
 * User: marcuse
 * Date: Jan 14, 2009
 * Time: 7:58:11 AM
 */
public class DriverTest {
    private Connection connection;
    static { BasicConfigurator.configure(); }

    public DriverTest() throws SQLException {
        try {
            Class.forName("org.drizzle.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        connection = DriverManager.getConnection("jdbc:drizzle://localhost:4427/test_units_jdbc");
        Statement stmt = connection.createStatement();
        try { stmt.execute("drop table t1"); } catch (Exception e) {}
        stmt.execute("create table t1 (id int not null primary key auto_increment, test varchar(20))");
        stmt.execute("insert into t1 (test) values (\"hej1\")");
        stmt.execute("insert into t1 (test) values (\"hej2\")");
        stmt.execute("insert into t1 (test) values (\"hej3\")");
        stmt.execute("insert into t1 (test) values (null)");
        
    }

    @Test
    public void doQuery() throws SQLException{
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select * from t1");
        for(int i=1;i<4;i++) {
            rs.next();
            assertEquals(String.valueOf(i),rs.getString(1));
            assertEquals("hej"+i,rs.getString("test"));
        }
        rs.next();
        assertEquals("NULL",rs.getString("test"));
    }

    @Test(expected = SQLException.class)
    public void badQuery() throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.executeQuery("whraoaooa");
    }
    @Test
    public void intOperations() {
        byte [] a = WriteBuffer.intToByteArray(99*256 + 77);

        assertEquals(a[0],77);
        assertEquals(a[1],99);
    }
    @Test
    public void longOperations() {
        byte [] a = WriteBuffer.longToByteArray(56*256*256*256 + 11*256*256 + 77*256 + 99);
        assertEquals(a[0],99);
        assertEquals(a[1],77);
        assertEquals(a[2],11);
        assertEquals(a[3],56);
    }
    @Test
    public void questionMarks() {
        String query = "1?234?6789??";
        List<Integer> qmIndexes = DrizzlePreparedStatement.getQuestionMarkIndexes(query);
        assertEquals(4, qmIndexes.size());
        for(Integer index : qmIndexes) {
            assertEquals('?',query.charAt(index)); 
        }
    }
    @Test
    public void insertTest() {
        String query = "aaaa?cccc?";
        String insert = "bbbb";
        String insert2 = "dddd";
        String inserted = DrizzlePreparedStatement.insertStringAt(query,insert,4);
        inserted = DrizzlePreparedStatement.insertStringAt(inserted,insert2,9+insert.length()-1);
        assertEquals("aaaabbbbccccdddd",inserted);
    }
    
    @Test
    public void preparedTest() throws SQLException {
        String query = "SELECT * FROM t1 WHERE test = ? and id = ?";
        PreparedStatement prepStmt = connection.prepareStatement(query);
        prepStmt.setString(1,"hej1");
        prepStmt.setInt(2,1);
        ResultSet results = prepStmt.executeQuery();
        String res = "";
        while(results.next()) {
            res=results.getString("test");
        }
        assertEquals("hej1",res);        
    }
    @Test
    public void updateTest() throws SQLException {
        String query = "UPDATE t1 SET test = ? where id = ?";
        PreparedStatement prepStmt = connection.prepareStatement(query);
        prepStmt.setString(1,"updated");
        prepStmt.setInt(2,3);
        int updateCount = prepStmt.executeUpdate();
        assertEquals(1,updateCount);
        String query2 = "SELECT * FROM t1 WHERE id=?";
        prepStmt = connection.prepareStatement(query2);
        prepStmt.setInt(1,3);
        ResultSet results = prepStmt.executeQuery();
        String result = "";
        while(results.next()) {
            result = results.getString("test");
        }
        assertEquals("updated",result);
    }
}
