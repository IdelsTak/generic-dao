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
package com.github.idelstak.genericdao.impl;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;

import com.github.idelstak.genericdao.ConnectionPool;
import com.github.idelstak.genericdao.RollbackException;

public class TranImpl {
    private static ThreadLocal<TranImpl> myTran = new ThreadLocal<TranImpl>();

    private Connection connection = null;
    private ConnectionPool connectionPool = null;
    private PrintWriter debugWriter = null;
    private boolean isDebugOverriden = false;

    public static void begin() throws RollbackException {
        TranImpl t = myTran.get();
        if (t != null)
            rollbackAndThrow("Cannot begin twice without commit or rollback (i.e., you were already in a transaction)!");
        myTran.set(new TranImpl());
    }

    public static void commit() throws RollbackException {
        TranImpl t = myTran.get();
        if (t == null)
            rollbackAndThrow("Not in a transaction");
        t.executeCommit();
    }

    public static boolean isActive() {
        return myTran.get() != null;
    }

    public static void rollback() {
        TranImpl t = myTran.get();
        if (t == null)
            throw new AssertionError("Not in a transaction");
        t.executeRollback();
    }

    public static void setDebugWriter(PrintWriter writer) {
        TranImpl t = myTran.get();
        if (t == null)
            throw new AssertionError("Not in a transaction");
        t.debugWriter = writer;
        t.isDebugOverriden = true;
    }

    static PrintWriter getDebugWriter(ConnectionPool cp) {
        TranImpl t = myTran.get();
        if (t == null)
            throw new AssertionError("Not in a transaction");
        if (t.isDebugOverriden)
            return t.debugWriter;
        return cp.getDebugWriter();
    }

    static void rollbackAndThrow(String message) throws RollbackException {
        rollbackAndThrow(new RollbackException(message));
    }

    static void rollbackAndThrow(Exception e) throws RollbackException {
        TranImpl t = myTran.get();
        if (t != null)
            t.executeRollback();
        if (e instanceof RollbackException)
            throw (RollbackException) e;
        throw new RollbackException(e);
    }

    static void rollbackAndThrow(String message, Exception e) throws RollbackException {
        TranImpl t = myTran.get();
        if (t != null)
            t.executeRollback();
        throw new RollbackException(message, e);
    }

    static void rollbackAndThrow(Connection con, Exception e, PrintWriter debug) throws RollbackException {
        if (isActive()) {
            rollbackAndThrow(e); // does not return
        }

        try {
            if (con != null && !con.getAutoCommit()) {
                con.rollback();
            }
        } catch (SQLException e2) {
            if (debug != null)
                e2.printStackTrace(debug);
        }

        try {
            if (con != null)
                con.close();
        } catch (SQLException e2) {
            if (debug != null)
                e2.printStackTrace(debug);
        }

        rollbackAndThrow(e); // does not return
    }

    static Connection getConnection() {
        TranImpl t = myTran.get();
        if (t == null)
            return null;
        return t.connection;
    }

    public static Connection join(ConnectionPool connectionPool, ConnPoolImpl cpImpl) throws RollbackException {
        TranImpl t = myTran.get();
        if (t == null)
            throw new RollbackException("Must be in a transaction.");

        if (t.connectionPool != null && t.connectionPool != connectionPool) {
            rollbackAndThrow("Cannot involve two connection pools in one transaction. Already involved: "
                    + t.connectionPool + ".  Trying to join: " + connectionPool);
        }

        if (t.connection != null)
            return t.connection;

        try {
            t.connectionPool = connectionPool;
            t.connection = cpImpl.getConnection();
            t.connection.setAutoCommit(false);
            if (!t.isDebugOverriden) {
                t.debugWriter = connectionPool.getDebugWriter();
            }
            if (t.debugWriter != null)
                t.debugWriter.println("join: using connection " + t.connection);
            return t.connection;
        } catch (SQLException e) {
            rollbackAndThrow(e);
            throw new AssertionError("Can't happen (rollbackAndThrow returned).");
        }
    }

    private TranImpl() {
        /* Private constructor forces use of static factory (TranImpl.begin()) */
    }

    private void executeCommit() throws RollbackException {
        myTran.set(null);

        if (connection != null) {
            try {
                if (debugWriter != null)
                    debugWriter.println("committing transaction");
                connection.commit();
                connection.setAutoCommit(true);
                connectionPool.releaseConnection(connection);
            } catch (SQLException e) {
                try {
                    connection.close();
                } catch (SQLException e2) {
                    if (debugWriter != null)
                        e2.printStackTrace();
                }
                throw new RollbackException(e);
            }
        }
    }

    private void executeRollback() {
        myTran.set(null);

        if (connection != null) {
            try {
                if (debugWriter != null)
                    debugWriter.println("rolling back transaction");
                connection.rollback();
                connection.setAutoCommit(true);
                connectionPool.releaseConnection(connection);
            } catch (SQLException e) {
                // Can't throw exception from application initiated rollback
                // In case of failure, connection will not be reused
                // We can print out the stack trace for the SQLException, but we
                // cannot throw it
                // Presumably, due to the broken connection or the closed
                // connection,
                // the server will rollback the transaction, but at least no
                // additional work
                // will be done on behalf of this transaction.

                try {
                    connection.close();
                } catch (SQLException e2) {
                    // Do not use debugPrintStackTrace as myTran is null, now.
                    if (debugWriter != null)
                        e2.printStackTrace();
                }

                // Do not use debugPrintStackTrace as myTran is null, now.
                if (debugWriter != null)
                    e.printStackTrace();
            }
        }
    }
}
