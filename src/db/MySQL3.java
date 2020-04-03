/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package db;

import java.sql.Connection;
import java.sql.SQLException;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import utils.Logging;
import utils.Props;

/**
 *
 * @author dennis
 */
public class MySQL3 {

    private static DataSource datasource;
    private static String connection;

    public MySQL3() {
        connection = "jdbc:mysql://" + Props.getDbHost() + ":" + Props.getDbPort() + "/" + Props.getDbName();
        Logging.info("jdbc:mysql://" + Props.getDbHost() + ":" + Props.getDbPort() + "/" + Props.getDbName());
        init(connection, Props.getDbUserName(), Props.getDbPassword(), Props.getMaxConnections());
    }

    public static void init(String url, String username, String password, int maxConnections) {
        PoolProperties p = new PoolProperties();
        p.setUrl(url);
        p.setDefaultAutoCommit(true);
        p.setDriverClassName("com.mysql.jdbc.Driver");
        p.setUsername(username);
        p.setPassword(password);
        p.setJmxEnabled(true);
        p.setTestWhileIdle(false);
        p.setTestOnBorrow(true);
        p.setValidationQuery("SELECT 1 FROM DUAL");
        p.setTestOnReturn(false);
        p.setValidationInterval(60000);
        p.setTimeBetweenEvictionRunsMillis(30000);
        p.setMaxActive(maxConnections);
        p.setInitialSize(10);
        p.setMaxWait(60000);
        p.setMinEvictableIdleTimeMillis(5000);
        p.setMinIdle(maxConnections);
        p.setMaxIdle(maxConnections);
        p.setLogAbandoned(false);
        p.setRemoveAbandoned(false);

        p.setJdbcInterceptors("org.apache.tomcat.jdbc.pool.interceptor.ConnectionState;"
                + "org.apache.tomcat.jdbc.pool.interceptor.StatementFinalizer;"
                + "org.apache.tomcat.jdbc.pool.interceptor.ResetAbandonedTimer");
        try {
            datasource = new DataSource(p);
            datasource.setPoolProperties(p);
            datasource.createPool();
        } catch (SQLException ex) {
            System.err.println(MySQL3.class.getName() + " " + ex);
        }

    }

    /**
     * <p>
     * Gets a MySQL database connection. Will throw an SQLException if there is
     * an error. The connection uses UTF-8 character encoding.
     * </p>
     *
     * <p>
     * The connection is obtained using the connect string:
     * jdbc:apache:commons:dbcp:poolName
     * </p>
     *
     * @return a MySQL connection object, null on error
     *
     * @throws SQLException if unable to get a connection from the connection
     * pool
     */
    public static Connection getConnection() throws SQLException {
        Connection conn = null;
        try {
            conn = datasource.getConnection();
            System.err.println("connection availability " + conn);
            Logging.info("connection availability " + conn);
        } catch (Exception e) {
            Logging.error("connection availability failed " + e.getMessage(), e);
            System.err.println("connection availability failed " + e);
            init(connection, Props.getDbUserName(), Props.getDbPassword(), Props.getMaxConnections());
            conn = datasource.getConnection();
        }
        return conn;
    }

    /**
     * <p>
     * Gets a MySQL database connection. Will throw an SQLException if there is
     * an error. The connection uses UTF-8 character encoding.
     * </p>
     *
     * <p>
     * The connection is obtained using the connect string:
     * jdbc:apache:commons:dbcp:poolName
     * </p>
     *
     * @return a MySQL connection object, null on error
     *
     * @throws SQLException if unable to get a connection from the connection
     * pool
     */
    public void releaseConnection() throws SQLException {
        try {
            datasource.close();
            System.err.println("connection closed");
        } catch (Exception e) {
            System.err.println("connection availability " + e);

        }
    }
}
