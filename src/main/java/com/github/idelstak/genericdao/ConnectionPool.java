/*
 * Copyright (c) 2012-2016 Jeffrey L. Eppinger.  All Rights Reserved.
 *     You may use, modify and share this code for non-commercial purposes
 *     as long a you comply with this license from Creative Commons:
 *     Summary of license: http://creativecommons.org/licenses/by-nc-sa/3.0
 *     Full Text of License: http://creativecommons.org/licenses/by-nc-sa/3.0/legalcode
 *     Specifically, if you distribute your code for non-educational purposes,
 *     you must include this copyright notice in your work.
 *     If you wish to have broader rights, you must contact the copyright holder.
 */
package com.github.idelstak.genericdao;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;

import com.github.idelstak.genericdao.impl.ConnPoolImpl;
import com.github.idelstak.genericdao.impl.MyPrintWriter;
import com.github.idelstak.genericdao.impl.TranImpl;

/**
 * An implementation of a connection pool for JDBC.
 * <p>
 * Rather than allocating a new connection every time we access the database,
 * we ask the connection pool for a connection and return it to the connection
 * pool when we're finished.  The connection pool will save the connection for
 * for subsequent reuse.  If there are no open connections to hand out, the
 * connection pool opens another one.  With some JDBC implementations, idle
 * connections eventually fail.  So, this connection pool closes idle
 * connections.  (See implementation for the current settings to determine
 * how long idle connections remain open.)
 * <p>
 * This connection pool coordinates with the <tt>Transaction</tt> class.
 * The <tt>getTransactionConnection()</tt> method can be used to get the connection that
 * is currently being used in the current transaction.  This enables you to execute your
 * own SQL statements in the same transaction and other GenericDAO calls.
 */
public class ConnectionPool {

    /**
     * Default max idle time for connections. Value is 5 seconds.
     */
    public final static long DEFAULT_MAX_IDLE_TIME = 5 * 1000;

    private ConnPoolImpl connPoolImpl;

    public ConnectionPool(String jdbcDriverName, String jdbcURL) {
        this(jdbcDriverName, jdbcURL, null, null);
    }

    public ConnectionPool(String jdbcDriverName, String jdbcURL, String user, String password) {
        if (jdbcDriverName == null) {
            throw new NullPointerException("jdbcDriverName");
        }

        if (jdbcURL == null) {
            throw new NullPointerException("jdbcURL");
        }

        // User and password can be null

        connPoolImpl = new ConnPoolImpl(jdbcDriverName, jdbcURL,
                                        user, password,
                                        DEFAULT_MAX_IDLE_TIME);
    }

    public Connection getConnection() throws SQLException {
        if (Transaction.isActive()) {
            throw new AssertionError(
                    "Cannot get separate connections during a transaction.  Try using getTransactionConnection().");
        }

        return connPoolImpl.getConnection();
    }

    public PrintWriter getDebugWriter() {
        return connPoolImpl.getDebugWriter();
    }

    /**
     * Gets the JDBC Driver Name that this connection pool uses.
     *
     * @return the JDBC Driver Name
     */
    public String getDriverName() {
        return connPoolImpl.getDriverName();
    }

    public boolean getLowerCaseColumnNames() {
        return getURL().contains("postgres");
    }

    /**
     * Get the maximum time that after which idle database connections are
     * closed.
     *
     * @return maximum idle time in milliseconds
     */
    public long getMaxIdleTime() {
        return connPoolImpl.getMaxIdleTime();
    }

    /**
     * Get the database connection used by the thread's current transaction.
     * The connection returned is the same one that a GenericDAO would be using
     * (if it's using this connection pool).
     * This permits you to execute SQL
     * statements in the same transaction as GenericDAO calls.
     * <p>
     * The connection is returned to the connection pool when the
     * transaction is committed or rolled back.  So do not call
     * <code>releaseConnection()</code> for connections in a transaction and
     * do not close the connection yourself.
     * <p>
     * You must use <code>Transaction.commit()</code> (or rollback) rather than
     * <code>connection.commit()</code> to end the transaction.
     * <p>
     * Note that current only one connection pool can be used in any transaction
     * (because the ACID properties cannot be guaranteed across SQL connections without a lot of work
     * and complication, e.g., using a two-phase commit protocol).
     *
     * @return a JDBC SQL Connection
     * @throws RollbackException
     *             if there is no active transaction, if there is an underlying
     *             SQL exception, or if there is already another connection pool
     *             using the currently active transaction. If this exception is
     *             thrown, the currently running transaction is rolled back.
     */
    public Connection getTransactionConnection() throws RollbackException {
        // If we're in a transaction, use the transaction's connection
        if (!TranImpl.isActive()) {
            throw new RollbackException("Must be in a transaction");
        }

        return TranImpl.join(this, connPoolImpl);
    }

    /**
     * Gets the JDBC URL that this connection pool uses.
     *
     * @return the JDBC URL
     */
    public String getURL() {
        return connPoolImpl.getURL();
    }

    /**
     * Gets the user name that this connection pool uses to log into the
     * database.
     *
     * @return the user name used to log into the database. Null is return if
     *         not user name was provided.
     */
    public String getUserName() {
        return connPoolImpl.getUserName();
    }

    /**
     * Returns a connection to the connection pool. The connection should have
     * been obtained using <tt>getConnection()</tt>.
     *
     * @param c connection to return to the connection pool.
     */
    public void releaseConnection(Connection c) {
        if (Transaction.isActive()) {
            throw new AssertionError(
                    "You cannot release connections in a transaction.  The transaction manager will do it when transaction.commit() is called.");
        }

        connPoolImpl.releaseConnection(c);
    }

    /**
     * Sets up an output stream to which debugging output can be printed.
     * This will call can be use to enable all instances of GenericDAO
     * (and <code>Transaction.commit()</code>)
     * to print out messages showing the SQL they are generating.
     *
     * @param out
     *          an output stream to which debugging info will be written.
     *          (Usually <code>System.out</code> is passed as the parameter.)
     *
     */
    public void setDebugOutput(OutputStream out) {
        if (out == null) {
            connPoolImpl.setDebugOutput(null);
        } else {
            connPoolImpl.setDebugOutput(new MyPrintWriter(out));
        }
    }

    /**
     * Changes the time after which idle database connections are closed. The
     * default time is 20 minutes (20*60*1000 milliseconds).
     *
     * @param millis
     *            time a connection can be idle before it will be closed
     */
    public void setMaxIdleTime(long millis) {
        connPoolImpl.setMaxIdleTime(millis);
    }

    /**
     * Returns a description of this connection pool, include the JDBC Driver
     * Name, the JDBC URL, and the user name used to log into the database.
     *
     * @return a description of this connection pool.
     */
    public String toString() {
        return "ConnectionPool(" + getDriverName() + ", " + getURL() + ", "
                + getUserName() + ")";
    }
}
