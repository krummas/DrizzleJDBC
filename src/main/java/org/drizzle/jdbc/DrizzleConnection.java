package org.drizzle.jdbc;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.io.IOException;

/**
 * User: marcuse
 * Date: Jan 14, 2009
 * Time: 7:47:37 AM
 */
public class DrizzleConnection implements Connection {
    private final Protocol protocol;
    private final String username;
    private final String host;
    private final int port;
    private final String database;
    public DrizzleConnection(String host, int port, String username, String password, String database) throws SQLException {
        this.username=username;
        this.port=port;
        this.host=host;
        this.database=database;
        try {
            protocol = new DrizzleProtocol(host,port,database,username,password);
        } catch (IOException e) {
            throw new SQLException("Could not connect: "+e.getMessage());
        } catch (UnauthorizedException e) {
            throw new SQLException("Not authorized: "+e.getMessage());
        }
    }
    public Statement createStatement() throws SQLException {
        return new DrizzleStatement(protocol,this);
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new DrizzlePreparedStatement(protocol,this,sql);
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Stored procedures not supported");
    }

    public String nativeSQL(String sql) throws SQLException {
        return sql;
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        
    }

    public boolean getAutoCommit() throws SQLException {
        return false;
    }

    public void commit() throws SQLException {

    }

    public void rollback() throws SQLException {

    }

    public void close() throws SQLException {
        try {
            protocol.close();
        } catch (IOException e) {
            throw new SQLException("Could not close socket: "+e.getMessage());
        }

    }

    /**
     * checks if the connection is closed
     * @return true if the connection is closed
     * @throws SQLException if the connection cannot be closed.
     */
    public boolean isClosed() throws SQLException {
        return protocol.isClosed();
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return new DrizzleDatabaseMetaData.Builder().
                                        url("jdbc:drizzle://"+host+":"+port+"/"+database).
                                        username(this.username).
                                        version(protocol.getVersion()).
                                        build();
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        protocol.setReadonly(readOnly);
    }

    public boolean isReadOnly() throws SQLException {
        return false;
    }

    public void setCatalog(String catalog) throws SQLException {

    }

    public String getCatalog() throws SQLException {
        return null;
    }

    public void setTransactionIsolation(int level) throws SQLException {

    }

    public int getTransactionIsolation() throws SQLException {
        return 0;
    }

    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    public void clearWarnings() throws SQLException {

    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return null;
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

    }

    public void setHoldability(int holdability) throws SQLException {

    }

    public int getHoldability() throws SQLException {
        return 0;
    }

    public Savepoint setSavepoint() throws SQLException {
        return null;
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        return null;
    }

    public void rollback(Savepoint savepoint) throws SQLException {

    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {

    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return null;
    }

    public PreparedStatement prepareStatement(String sql, int columnIndexes[]) throws SQLException {
        return null;
    }

    public PreparedStatement prepareStatement(String sql, String columnNames[]) throws SQLException {
        return null;
    }

    public Clob createClob() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Blob createBlob() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public NClob createNClob() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public SQLXML createSQLXML() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean isValid(int i) throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setClientInfo(String s, String s1) throws SQLClientInfoException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public String getClientInfo(String s) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Properties getClientInfo() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Array createArrayOf(String s, Object[] objects) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Struct createStruct(String s, Object[] objects) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public <T> T unwrap(Class<T> tClass) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean isWrapperFor(Class<?> aClass) throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
