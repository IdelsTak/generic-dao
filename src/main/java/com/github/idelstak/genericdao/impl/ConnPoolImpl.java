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
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ConnPoolImpl {

    private String jdbcDriverName;
    private String jdbcURL;
    private String user;
    private String password;
    private PrintWriter debugWriter = null;

    // A helper class to keep track of connections in the pool and the last time they were used.
    private static class MyConnTime {

        Connection conn;
        long lastUsed;  // time in millis
    }
    // The following variables are synchronized on connections.
    private List<MyConnTime> connections = new ArrayList<MyConnTime>();
    private Thread cleanerThread = null;
    private long maxIdleTime;
    private long lastGetConnectionTime;

    public ConnPoolImpl(String jdbcDriverName, String jdbcURL, String user, String password, long maxIdleTime) {
        this.jdbcDriverName = jdbcDriverName;
        this.jdbcURL = jdbcURL;
        this.user = user;
        this.password = password;
        this.maxIdleTime = maxIdleTime;
    }

    public Connection getConnection() throws SQLException {
        // If there is already a connection in the pool, return it
        synchronized (connections) {
            lastGetConnectionTime = System.currentTimeMillis();
            if (connections.size() > 0) {
                MyConnTime myConn = connections.remove(connections.size() - 1);
                return myConn.conn;
            }
        }

        // Otherwise, make a new connection and return it
        try {
            Class.forName(jdbcDriverName);
        } catch (ClassNotFoundException e) {
            throw new AssertionError("Could not load database driver: " + e.toString());
        }

        if (user == null) {
            return DriverManager.getConnection(jdbcURL);
        }
        return DriverManager.getConnection(jdbcURL, user, password);
    }

    public synchronized PrintWriter getDebugWriter() {
        return debugWriter;
    }

    public String getDriverName() {
        return jdbcDriverName;
    }

    public long getMaxIdleTime() {
        synchronized (connections) {
            return maxIdleTime;
        }
    }

    public String getURL() {
        return jdbcURL;
    }

    public String getUserName() {
        return user;
    }

    public void releaseConnection(Connection c) {
        if (TranImpl.isActive()) {
            throw new AssertionError("You cannot release connections in a transaction.  The transaction manager");
        }

        MyConnTime myConn = new MyConnTime();
        myConn.conn = c;
        myConn.lastUsed = System.currentTimeMillis();

        synchronized (connections) {
            connections.add(myConn);

            if (cleanerThread == null) {
                cleanerThread = new CleanerThread();
                cleanerThread.start();
            }
        }
    }

    public synchronized void setDebugOutput(PrintWriter writer) {
        debugWriter = writer;
    }

    public void setMaxIdleTime(long millis) {
        synchronized (connections) {
            maxIdleTime = millis;
        }
    }

    private class CleanerThread extends Thread {

        public void run() {
            while (true) {
                long now = System.currentTimeMillis();
                Connection connectionToClose;
                long sleepTime = 0;

                // We won't want to close a connection or sleep in the synchronized block
                // We just want to figure out which we need to do
                synchronized (connections) {
                    if (connections.isEmpty()) {
                        if (now - lastGetConnectionTime > maxIdleTime) {
                            cleanerThread = null;
                            return;
                        }

                        sleepTime = maxIdleTime;
                        connectionToClose = null;
                    } else {
                        MyConnTime mostIdle = connections.get(0);
                        if (now - mostIdle.lastUsed > maxIdleTime) {
                            connections.remove(0);
                            connectionToClose = mostIdle.conn;
                        } else {
                            sleepTime = maxIdleTime - now + mostIdle.lastUsed + 1;
                            connectionToClose = null;
                        }
                    }
                }

                // After the loop, either connectionToClose or sleepTime will be set (or we thread will have exited.)
                try {
                    if (connectionToClose != null) {
                        connectionToClose.close();
                    } else {
                        Thread.sleep(sleepTime);
                    }
                } catch (InterruptedException | SQLException e) {
                    PrintWriter writer = getDebugWriter();
                    if (writer != null) {
                        e.printStackTrace(writer);
                    }
                }
            }
        }
    }
}
