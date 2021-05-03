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
import com.github.idelstak.genericdao.impl.matcharg.MatchArgLeafNode;
import com.github.idelstak.genericdao.impl.matcharg.MatchOp;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MySQLImpl<B> extends GenericDAOImpl<B> {

    public MySQLImpl(Class<B> beanClass, String tableName,
            ConnectionPool connectionPool) throws DAOException {
        super(beanClass, tableName, connectionPool);
    }

    protected String getBlobTypeDeclaration() {
        return "LONGBLOB";
    }

    protected String getDateTimeTypeDeclaration() {
        return "DATETIME";
    }

    protected String getSerialTypeDeclaration() {
        return "INT NOT NULL AUTO_INCREMENT";
    }

    @Override
    protected String getBigSerialTypeDeclaration() {
        return "BIGINT NOT NULL AUTO_INCREMENT";
    }

    protected String getDefaultSchemaName() {
        return null;
    }

    protected String getLikeOperator() {
        return "LIKE BINARY";
    }

    protected String getLikeIgnoringCaseOperator() {
        return "LIKE";
    }

    protected String getNullSafeEqualsOperator() {
        return "<=> BINARY";
    }

    protected Object fetchMinMaxValue(Connection con, MatchArgLeafNode arg,
            String tableName, PrintWriter debug) throws SQLException {
        Property prop = arg.getProperty();
        StringBuilder sql = new StringBuilder();
        sql.append("select ");
        if (arg.getOp() == MatchOp.MAX) {
            sql.append("max(");
        } else {
            sql.append("min(");
        }
        if (prop.getType() == String.class) {
            sql.append("binary ");
        }
        sql.append(prop.getName());
        sql.append(") as matchValue from ");
        sql.append(tableName);
        sql.append(" for update");

        if (debug != null) {
            debug.println("fixMaxMin: " + sql);
        }
        Object matchValue;
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(sql.toString());
            // If no rows in the table, then NULL is returned for max or min
            // operator
            if (!rs.next()) {
                throw new AssertionError("No row returned.");
            }   matchValue = rs.getObject("matchValue");
        }

        return matchValue;
    }

    protected String getVarCharTypeDeclaration(int maxStringLength) {
        return "VARCHAR (" + maxStringLength + ")";
    }

    @Override
    protected Object createAutoIncrement(Connection con, B bean, PrintWriter debug) throws SQLException, RollbackException {

        StringBuilder b = new StringBuilder();
        b.append("INSERT INTO ");
        b.append(tableName);
        b.append(" (");
        b.append(columnNamesCommaSeparated);
        b.append(") values (default ");
        if (nonPrimaryKeyColumnQuestionsCommaSeparated.length() > 0) {
            b.append(", ");
            b.append(nonPrimaryKeyColumnQuestionsCommaSeparated);
        }
        b.append(")");

        String sql = b.toString();
        if (debug != null) {
            debug.println("createAutoIncrement: " + sql);
        }
        try (PreparedStatement pstmt = con.prepareStatement(sql)) {
            for (int i = 1; i < properties.length; i++) {
                Object value = getBeanValue(bean, properties[i]);
                if (debug != null) {
                    debug.println("   set arg #" + i + " to " + value);
                }
                checkMaxStringLength(properties[i], value);
                pstmt.setObject(i, value);
            }
            pstmt.executeUpdate();
        }

        Object id;
        try (Statement stmt = con.createStatement()) {
            if (debug != null) {
                debug.println("createAutoIncrement: SELECT LAST_INSERT_ID()");
            }   ResultSet rs = stmt.executeQuery("SELECT LAST_INSERT_ID()");
            rs.next();
            id = rs.getObject("LAST_INSERT_ID()");
            if (debug != null) {
                debug.println("createAutoIncrement: ...LAST_INSERT_ID()=" + id);
            }
        }

        if (id instanceof BigInteger) {
            BigInteger bigInt = (BigInteger) id;
            id = bigInt.longValue();
        }

        if (properties[0].getType() == long.class) {
            return id;
        }

        long x = (Long) id;
        int i = (int) x;
        return i;
    }
}
