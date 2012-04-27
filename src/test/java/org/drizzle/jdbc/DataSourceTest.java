package org.drizzle.jdbc;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.sql.Connection;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Sep 1, 2009
 * Time: 7:03:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class DataSourceTest {
  
    DataSource ds;
    public void testDrizzleDataSource() throws SQLException {
        if (ConnectionCheck.host.contains(":"))
        {
            String copyhost_forsplit=ConnectionCheck.host;
            String Brk[]=copyhost_forsplit.split(":");
            int convert=Integer.parseInt(Brk[1]);
            ds = new DrizzleDataSource(Brk[0],convert,"test_units_jdbc");
        }
        else
        {
            ds = new DrizzleDataSource(ConnectionCheck.host,3307,"test_units_jdbc");
        }
        Connection connection = ds.getConnection("root", null);
        assertEquals(connection.isValid(0),true);
    }
    @Test
    public void testDrizzleDataSource2() throws SQLException {
        if (ConnectionCheck.host.contains(":"))
        {
            String copyhost_forsplit=ConnectionCheck.host;
            String Brk[]=copyhost_forsplit.split(":");
            int convert=Integer.parseInt(Brk[1]);
            ds = new DrizzleDataSource(Brk[0],convert,"test_units_jdbc");
        }
        else
        {
            ds = new DrizzleDataSource(ConnectionCheck.host,3307,"test_units_jdbc");
        }
        Connection connection = ds.getConnection("root","");
        assertEquals(connection.isValid(0),true);
    }
}
