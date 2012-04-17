/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.drizzle.jdbc;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.sql.*;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 *
 * @author waqas
 */
public class ConnectionCheck {
    public static String host="localhost";
    public static String mysql_host="localhost";
    public static Connection Get_ConnectionDrizzle() throws SQLException
    {
        Connection connection;
        if (host.contains(":"))
        {
            
            connection = DriverManager.getConnection("jdbc:drizzle://root@" + host + "/test_units_jdbc");
        }
        else
        {
            
            connection = DriverManager.getConnection("jdbc:drizzle://root@" + host + ":4427/test_units_jdbc");
        }
        return connection;
    }
    public static Connection Get_ConnectionDrizzle(String Property) throws SQLException
    {
        Connection connection;
        if (host.contains(":"))
        {
            
            connection = DriverManager.getConnection("jdbc:drizzle://root@" + host + "/test_units_jdbc"+Property);
        }
        else
        {
            
            connection = DriverManager.getConnection("jdbc:drizzle://root@" + host + ":4427/test_units_jdbc"+Property);
        }
        return connection;
    }
    public static Connection Get_ConnectionMySQL() throws SQLException
    {
        Connection connection;
        if (mysql_host.contains(":"))
        {
            
            connection = DriverManager.getConnection("jdbc:mysql:thin://"+mysql_host+"/test_units_jdbc");
        }
        else
        {
            
            connection = DriverManager.getConnection("jdbc:mysql:thin://"+mysql_host+":3306/test_units_jdbc");
        }
        return connection;
    }
        public static Connection Get_ConnectionMySQL_WithUsername(String Username) throws SQLException
    {
        Connection connection;
        if (mysql_host.contains(":"))
        {
            
            connection = DriverManager.getConnection("jdbc:mysql:thin://"+Username+"@"+mysql_host+"/test_units_jdbc");
        }
        else
        {
            
            connection = DriverManager.getConnection("jdbc:mysql:thin://"+Username+"@"+mysql_host+":3306/test_units_jdbc");
        }
        return connection;
    }
    public static Connection Get_ConnectionMySQL(String Property) throws SQLException
    {
        Connection connection;
            if (mysql_host.contains(":"))
            {
            
                connection = DriverManager.getConnection("jdbc:mysql:thin://"+mysql_host+"/test_units_jdbc"+Property);
            }
            else
            {
            
                connection = DriverManager.getConnection("jdbc:mysql:thin://"+mysql_host+":3306/test_units_jdbc"+Property);
            }
        
        return connection;
    }
    
    
}
