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

import com.github.idelstak.genericdao.ConnectionPool;
import com.github.idelstak.genericdao.DAOException;
import com.github.idelstak.genericdao.RollbackException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class GenericViewDAOImpl<B> {

    protected ConnectionPool connectionPool;
    protected Class<B> beanClass;
    protected Property[] properties;

    public GenericViewDAOImpl(Class<B> beanClass, ConnectionPool connectionPool) throws DAOException {
        // Check for null values and throw here (it's less confusing for the caller)
        if (beanClass == null) {
            throw new NullPointerException("beanClass");
        }
        if (connectionPool == null) {
            throw new NullPointerException("connectionPool");
        }

        this.connectionPool = connectionPool;
        this.beanClass = beanClass;
        properties = Property.deriveProperties(beanClass, connectionPool.getLowerCaseColumnNames());

        PrintWriter debug = connectionPool.getDebugWriter();
        if (debug != null) {
            for (Property p : properties) {
                debug.println(p);
            }
        }
    }

    public B[] executeQuery(String sql, Object... args) throws RollbackException {
        Connection con = null;
        try {
            con = myJoin();
            PrintWriter debug = getDebug();

            if (debug != null) {
                debug.println("executeQuery: sql = " + sql);
            }
            List<B> beanList;
            try (PreparedStatement pstmt = con.prepareStatement(sql)) {
                for (int i = 0; i < args.length; i++) {
                    if (debug != null) {
                        debug.println("   set arg #" + (i + 1) + " to " + args[i]);
                    }
                    pstmt.setObject(i + 1, args[i]);
                }
                try (ResultSet rs = pstmt.executeQuery()) {
                    beanList = new ArrayList<>();
                    while (rs.next()) {
                        B bean = newBean();
                        for (Property prop : properties) {
                            Object value = rs.getObject(prop.getColumnName());
                            value = fixDate(value);
                            setBeanValue(bean, prop, value);
                        }
                        beanList.add(bean);
                    }
                }
            }
            myRelease(con, debug);

            B[] beanArray = newArray(beanList.size());
            beanList.toArray(beanArray);
            if (debug != null) {
                debug.println("executeQuery: returning " + beanArray.length + " beans");
            }
            return beanArray;
        } catch (SQLException e) {
            TranImpl.rollbackAndThrow(con, e, getDebug());
            throw new AssertionError("rollbackAndThrow returned (can't happen)");
        }
    }

    public String[] getPropertyNames() {
        String[] names = new String[properties.length];

        for (int i = 0; i < properties.length; i++) {
            names[i] = properties[i].getName();
        }

        return names;
    }

    protected PrintWriter getDebug() {
        if (TranImpl.isActive()) {
            return TranImpl.getDebugWriter(connectionPool);
        }
        return connectionPool.getDebugWriter();
    }

    protected Connection myJoin() throws RollbackException, SQLException {
        if (TranImpl.isActive()) {
            Connection c = connectionPool.getTransactionConnection();
            PrintWriter debug = TranImpl.getDebugWriter(connectionPool);
            if (debug != null) {
                debug.println("joining transaction");
            }
            return c;
        }

        Connection c = connectionPool.getConnection();
        PrintWriter debug = connectionPool.getDebugWriter();
        if (debug != null) {
            debug.println("getting connection: " + c);
        }
        return c;
    }

    protected void myRelease(Connection con, PrintWriter debug) throws RollbackException, SQLException {
        if (TranImpl.isActive()) {
            return;
        }

        connectionPool.releaseConnection(con);
        if (debug != null) {
            debug.println("releasing connection: " + con);
        }
    }

    protected Object getBeanValue(Object bean, Property property) throws RollbackException {
        Method getter = property.getGetter();
        try {
            return getter.invoke(bean);
        } catch (IllegalAccessException e) {
            TranImpl.rollbackAndThrow("IllegalAccessException when getting "
                    + property + " from bean=" + bean, e);
        } catch (InvocationTargetException e) {
            TranImpl.rollbackAndThrow("InvocationTargetException when getting "
                    + property + " from bean=" + bean, e);
        }

        throw new AssertionError("Should not get here.");
    }

    @SuppressWarnings("unchecked")
    protected B[] newArray(int size) {
        Object array = java.lang.reflect.Array.newInstance(beanClass, size);
        return (B[]) array;
    }

    protected B newBean() throws RollbackException {
        try {
            return beanClass.newInstance();
        } catch (IllegalAccessException | InstantiationException e) {
            TranImpl.rollbackAndThrow("Error instantiating " + beanClass.getName(), e);
        }

        throw new AssertionError("rollbackAndThrow returned (can't happen)");
    }

    protected void setBeanValue(B bean, Property property, Object value) throws RollbackException {
        try {
            Method setter = property.getSetter();
            setter.invoke(bean, value);
        } catch (NullPointerException e) {
            TranImpl.rollbackAndThrow("NullPointerException when setting "
                    + property + " to value=" + value + " for bean=" + bean, e);
        } catch (IllegalAccessException e) {
            TranImpl.rollbackAndThrow("IllegalAccessException when setting "
                    + property + " to value=" + value + " for bean=" + bean, e);
        } catch (InvocationTargetException e) {
            TranImpl.rollbackAndThrow("InvocationTargetException when setting "
                    + property + " to value=" + value + " for bean=" + bean, e);
        } catch (IllegalArgumentException e) {
            TranImpl.rollbackAndThrow("IllegalArgumentException when setting "
                    + property + " to value=" + value + (value == null ? "" : (" type=" + value.getClass().getName()) + ")") + " for bean=" + bean, e);
        }
    }

    private Object fixDate(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof java.sql.Timestamp) {
            java.sql.Timestamp ts = (java.sql.Timestamp) value;
            return new java.util.Date(ts.getTime());
        }

        return value;
    }
}
