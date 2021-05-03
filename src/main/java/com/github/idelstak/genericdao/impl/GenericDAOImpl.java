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

import java.io.File;
import java.io.PrintWriter;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import com.github.idelstak.genericdao.ConnectionPool;
import com.github.idelstak.genericdao.DAOException;
import com.github.idelstak.genericdao.DuplicateKeyException;
import com.github.idelstak.genericdao.MatchArg;
import com.github.idelstak.genericdao.RollbackException;
import com.github.idelstak.genericdao.Transaction;
import com.github.idelstak.genericdao.impl.matcharg.MatchArgInternalNode;
import com.github.idelstak.genericdao.impl.matcharg.MatchArgLeafNode;
import com.github.idelstak.genericdao.impl.matcharg.MatchOp;
import java.security.NoSuchAlgorithmException;

public abstract class GenericDAOImpl<B> extends GenericViewDAOImpl<B> {

    // Initialized by constructor
    protected String tableName;
    protected String columnNamesCommaSeparated;
    protected String nonPrimaryKeyColumnQuestionsCommaSeparated;
    // Initialized by constructor
    private Property[] primaryKeyProperties;
    private Property[] nonPrimaryKeyProperties;
    private String schemaName;
    private String tableNameWithoutSchema;
    private String columnQuestionsCommaSeparated;
    private String nonPrimaryKeyColumnNamesEqualsQuestionsCommaSeparated;
    private String primaryKeyColumnNamesEqualsQuestionsAndSeparated;
    private String primaryKeyColumnNamesCommaSeparated;

    protected GenericDAOImpl(Class<B> beanClass, String tableName, ConnectionPool connectionPool) throws DAOException {
        super(beanClass, connectionPool);

        // Check for null values and throw here (it's less confusing for the
        // caller)
        if (tableName == null) {
            throw new NullPointerException("tableName");
        }

        this.tableName = tableName.toLowerCase();

        primaryKeyProperties = extractProperties(true);
        nonPrimaryKeyProperties = extractProperties(false);

        if (primaryKeyProperties.length == 0) {
            throw new DAOException(
                    "No primary key properties specified in the bean: "
                    + beanClass.getName());
        }

        if (this.tableName.contains(".")) {
            int dotPos = this.tableName.indexOf('.');
            schemaName = this.tableName.substring(0, dotPos);
            tableNameWithoutSchema = this.tableName.substring(dotPos + 1);
        } else {
            schemaName = getDefaultSchemaName();
            tableNameWithoutSchema = this.tableName;
        }

        columnNamesCommaSeparated = concatNameSepSuff(properties, ", ", "");
        columnQuestionsCommaSeparated = concatTokenSep(properties, "?", ", ");
        nonPrimaryKeyColumnNamesEqualsQuestionsCommaSeparated = concatNameSepSuff(nonPrimaryKeyProperties, "=?, ", "=?");
        nonPrimaryKeyColumnQuestionsCommaSeparated = concatTokenSep(nonPrimaryKeyProperties, "?", ", ");
        primaryKeyColumnNamesEqualsQuestionsAndSeparated = concatNameSepSuff(primaryKeyProperties, "=? AND ", "=?");
        primaryKeyColumnNamesCommaSeparated = concatNameSepSuff(primaryKeyProperties, ", ", "");
    }

    /**
     * Creates this table in the database. This method uses introspection to determine the properties of <tt>B</tt> and creates a database table that
     * can store instances of <tt>B</tt>.
     *
     * @param primaryKeyPropertyNames the names of the properties of <tt>B</tt> that comprise the primary key. Primary key property values cannot be
     * <tt>null</tt> and each instance of <tt>B</tt> stored in this table must have a unique primary key.
     *
     * @throws DAOException if the table cannot be created. Possible reasons the table cannot be created include: the table already exists, there are
     * missing getters, setters, or constructors for the primary key properties, this <tt>BeanTable</tt> cannot determine how to map a property to the
     * database table, there is an error connecting to the database.
     */
    public synchronized void createTable() throws DAOException {
        StringBuilder b = new StringBuilder();
        b.append("create table ");
        b.append(tableName);
        b.append(" (");
        for (int i = 0; i < properties.length; i++) {
            if (i > 0) {
                b.append(", ");
            }
            Property prop = properties[i];
            if (prop.isPrimaryKeyProperty() && prop.getType() == int.class
                    && primaryKeyProperties.length == 1) {
                b.append(prop.getName());
                b.append(' ');
                b.append(getSerialTypeDeclaration());
            } else if (prop.isPrimaryKeyProperty()
                    && prop.getType() == long.class
                    && primaryKeyProperties.length == 1) {
                b.append(prop.getName());
                b.append(' ');
                b.append(getBigSerialTypeDeclaration());
            } else {
                b.append(prop.getColumnName());
                b.append(' ');
                b.append(javaToSql(prop.getColumnType(), prop,
                        prop.getColumnMaxStrLen()));
            }
        }

        if (primaryKeyProperties.length > 0) {
            b.append(", PRIMARY KEY(");
            b.append(primaryKeyColumnNamesCommaSeparated);
            b.append(')');
        }

        b.append(')');

        PrintWriter debug = connectionPool.getDebugWriter();
        Connection con = null;
        try {
            con = connectionPool.getConnection();
            Statement stmt = con.createStatement();
            if (debug != null) {
                debug.println("createTable: " + b);
            }
            stmt.executeUpdate(b.toString());
            stmt.close();
        } catch (SQLException e) {
            try {
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e2) {
                if (debug != null) {
                    e.printStackTrace(debug);
                }
            }
            throw new DAOException("Error creating table \"" + tableName
                    + "\": " + e.getMessage(), e);
        }
    }

    /**
     * Deletes this table from the database. Also deletes any auxiliary tables (that contain array data). This call does not validate the table (see
     * <tt>getFactory()</tt>). This call does not throw an exception if the table does not exist.
     *
     * @throws DAOException if there is an error connecting to the database.
     */
    public synchronized void deleteTable() throws DAOException {
        PrintWriter debug = connectionPool.getDebugWriter();
        Connection con = null;
        try {
            con = connectionPool.getConnection();
            Statement stmt = con.createStatement();
            String sql = "DROP TABLE " + tableName;
            if (debug != null) {
                debug.println("deleteTable: " + sql);
            }
            stmt.executeUpdate(sql);
            stmt.close();
            connectionPool.releaseConnection(con);
        } catch (SQLException e) {
            try {
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e2) {
                if (debug != null) {
                    e2.printStackTrace(debug);
                }
            }
            throw new DAOException(e);
        }
    }

    /**
     * Checks to see if this table exists in the database.
     *
     * This call does not check to see if the table will successfully validate (see <tt>getFactory()</tt>).
     *
     * @return true if the table exists.
     * @throws DAOException if there is an error connecting to the database.
     */
    public boolean tableExists() throws DAOException {
        PrintWriter debug = connectionPool.getDebugWriter();
        Connection con = null;
        try {
            con = connectionPool.getConnection();
            DatabaseMetaData metaData = con.getMetaData();
            if (debug != null) {
                debug.println("tableExists(" + schemaName + ","
                        + tableNameWithoutSchema + "): metaData.getTables()");
            }
            boolean answer;
            try (ResultSet rs = metaData.getTables(null, schemaName, tableNameWithoutSchema, null)) {
                answer = false;
                while (rs.next() && !answer) {
                    String s = rs.getString("TABLE_NAME");
                    boolean isWindows = (File.separatorChar == '\\');
                    if (isWindows) {
                        // It's windows...case insensitive matching
                        if (tableNameWithoutSchema.equalsIgnoreCase(s)) {
                            answer = true;
                        }
                    } else {
                        // It's Unix...case counts
                        if (tableNameWithoutSchema.equals(s)) {
                            answer = true;
                        }
                    }
                }
            }
            connectionPool.releaseConnection(con);

            if (debug != null) {
                debug.println("tableExists(" + schemaName + ","
                        + tableNameWithoutSchema + "): returns " + answer);
            }

            return answer;
        } catch (SQLException e) {
            try {
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e2) {
            }
            throw new DAOException(e);
        }
    }

    public String computeDigest(B bean) throws RollbackException {
        try {
            if (bean == null) {
                throw new NullPointerException("The \"bean\" argument is null");
            }

            MessageDigest md = MessageDigest.getInstance("SHA1");

            for (Property property : properties) {
                Object beanValue = getBeanValue(bean, property);
                byte[] bytes = Encode.getBytes(beanValue);
                md.update(bytes);
            }

            byte[] digestBytes = md.digest();

            // Format the digest as a String
            StringBuilder digestSB = new StringBuilder();
            for (int i = 0; i < digestBytes.length; i++) {
                int lowNibble = digestBytes[i] & 0x0f;
                int highNibble = (digestBytes[i] >> 4) & 0x0f;
                digestSB.append(Integer.toHexString(highNibble));
                digestSB.append(Integer.toHexString(lowNibble));
            }

            return digestSB.toString();
        } catch (RollbackException | NullPointerException | NoSuchAlgorithmException e) {
            TranImpl.rollbackAndThrow(e);
            throw new AssertionError("rollbackAndThrow() returned");
        }
    }

    public void create(B bean) throws RollbackException {
        Connection con = null;
        try {
            con = myJoin();
            PrintWriter debug = getDebug();

            if (primaryKeyProperties.length == 1
                    && (primaryKeyProperties[0].getType() == int.class
                    || primaryKeyProperties[0].getType() == long.class)) {
                Object id = createAutoIncrement(con, bean, debug);
                setBeanValue(bean, properties[0], id);

                myRelease(con, debug);
                return;
            }

            String sql = "INSERT INTO " + tableName + " ("
                    + columnNamesCommaSeparated + ") values ("
                    + columnQuestionsCommaSeparated + ")";
            if (debug != null) {
                debug.println("create: " + sql);
            }
            PreparedStatement pstmt = con.prepareStatement(sql);
            for (int i = 0; i < properties.length; i++) {
                Object value = getBeanValue(bean, properties[i]);
                if (debug != null) {
                    debug.println("   set arg #" + (i + 1)
                            + " (" + properties[i] + ") to " + value);
                }
                checkMaxStringLength(properties[i], value);
                pstmt.setObject(i + 1, value);
            }
            pstmt.executeUpdate();
            pstmt.close();

            myRelease(con, debug);
        } catch (SQLException e) {
            if (e.getMessage().startsWith("Duplicate")) {
                TranImpl.rollbackAndThrow(con, new DuplicateKeyException(e.getMessage()), getDebug());
            }
            TranImpl.rollbackAndThrow(con, e, getDebug());
        } catch (Exception e) {
            TranImpl.rollbackAndThrow(con, e, getDebug());
        }
    }

    public void delete(Object... primaryKeyValues) throws RollbackException {
        validatePrimaryKeyValues(primaryKeyValues); // throws RollbackException
        // if problems

        Connection con = null;
        try {
            con = myJoin();
            PrintWriter debug = getDebug();

            String whereClause = " WHERE "
                    + primaryKeyColumnNamesEqualsQuestionsAndSeparated;
            String sql = "DELETE FROM " + tableName + whereClause;
            if (debug != null) {
                debug.println("delete: " + sql);
            }
            PreparedStatement pstmt = con.prepareStatement(sql);
            for (int i = 0; i < primaryKeyValues.length; i++) {
                if (debug != null) {
                    debug.println("   set arg #" + (i + 1) + " to "
                            + primaryKeyValues[i]);
                }
                pstmt.setObject(i + 1, primaryKeyValues[i]);
            }

            int num = pstmt.executeUpdate();
            pstmt.close();

            if (num != 1) {
                StringBuilder b = new StringBuilder();
                for (int i = 0; i < primaryKeyValues.length; i++) {
                    if (i > 0) {
                        b.append(",");
                    }
                    b.append(primaryKeyValues[i]);
                }
                if (num == 0) {
                    throw new RollbackException("No row with primary key = \""
                            + b + "\".");
                }
                throw new RollbackException("AssertionError: There are " + num
                        + " rows with primary key = \"" + b + "\".");
            }

            myRelease(con, debug);
        } catch (Exception e) {
            TranImpl.rollbackAndThrow(con, e, getDebug());
        }
    }

    public int getBeanCount() throws RollbackException {
        Connection con = null;
        try {
            con = myJoin();
            PrintWriter debug = getDebug();

            Statement stmt = con.createStatement();
            String sql = "SELECT COUNT(*) FROM " + tableName;
            if (debug != null) {
                debug.println("getBeanCount: " + sql);
            }
            ResultSet rs = stmt.executeQuery(sql);
            rs.next();
            int answer = rs.getInt(1);
            stmt.close();

            myRelease(con, debug);

            if (debug != null) {
                debug.println("getBeanCount: returning " + answer);
            }
            return answer;
        } catch (Exception e) {
            TranImpl.rollbackAndThrow(con, e, getDebug());
            throw new AssertionError("rollbackAndThrow returned (can't happen)");
        }
    }

    public B[] match(MatchArg... constraints) throws RollbackException {
        MatchArgTree sepMatchArgs = MatchArgTree.buildTree(properties,
                MatchArg.and(constraints)); // throws RollbackException in case
        // of problems
        if (!TranImpl.isActive() && sepMatchArgs.containsMaxOrMin()) {
            // If we have a max or min, we must do match in a transaction so we
            // can
            // first fetch max and min values and then match the other
            // constraints
            Transaction.begin();
            B[] answer = sqlMatch(sepMatchArgs); // throws RollbackException in
            // case of problems
            Transaction.commit();
            return answer;
        }

        return sqlMatch(sepMatchArgs); // throws RollbackException in case of
        // problems
    }

    public B read(Object... primaryKeyValues) throws RollbackException {
        validatePrimaryKeyValues(primaryKeyValues); // throws RollbackException
        // in case of problems

        PrintWriter debug = getDebug();
        if (debug != null) {
            StringBuilder b = new StringBuilder();
            for (int i = 0; i < primaryKeyValues.length; i++) {
                if (i > 0) {
                    b.append(',');
                }
                b.append(primaryKeyValues[i]);
            }
            debug.println("read: " + b);
        }

        try {
            MatchArg[] matchArgs = new MatchArg[primaryKeyProperties.length];
            for (int i = 0; i < primaryKeyProperties.length; i++) {
                matchArgs[i] = MatchArg.equals(
                        primaryKeyProperties[i].getName(), primaryKeyValues[i]);
            }

            B[] list = match(matchArgs);
            if (list.length == 0) {
                return null;
            }
            if (list.length == 1) {
                return list[0];
            }

            StringBuilder b = new StringBuilder();
            for (int i = 0; i < primaryKeyValues.length; i++) {
                if (i > 0) {
                    b.append(',');
                }
                b.append(primaryKeyValues[i]);
            }
            throw new RollbackException("AssertionError: " + list.length
                    + " records with same primary key: " + b);
        } catch (Exception e) {
            TranImpl.rollbackAndThrow(e);
            throw new AssertionError("rollbackAndThrow returned");
        }
    }

    public void update(B bean) throws RollbackException {
        Connection con = null;
        try {
            con = myJoin();
            PrintWriter debug = getDebug();

            String sql = "UPDATE " + tableName + " SET "
                    + nonPrimaryKeyColumnNamesEqualsQuestionsCommaSeparated
                    + " WHERE "
                    + primaryKeyColumnNamesEqualsQuestionsAndSeparated;
            if (debug != null) {
                debug.println("update: " + sql);
            }
            PreparedStatement pstmt = con.prepareStatement(sql);
            int i = 0;
            for (Property p : properties) {
                if (!p.isPrimaryKeyProperty()) {
                    i = i + 1;
                    Object value = getBeanValue(bean, p);
                    if (debug != null) {
                        debug.println("   set arg #" + i + " (" + p + ") to " + value);
                    }
                    checkMaxStringLength(p, value);
                    pstmt.setObject(i, value);
                }
            }
            for (Property p : primaryKeyProperties) {
                i = i + 1;
                Object value = getBeanValue(bean, p);
                if (debug != null) {
                    debug.println("   set arg #" + i + " (" + p + ") to " + value);
                }
                checkMaxStringLength(p, value);
                pstmt.setObject(i, value);
            }
            int count = pstmt.executeUpdate();
            pstmt.close();
            if (count != 1) {
                throw new RollbackException("AssertionError: Incorrect number of rows updated: " + count);
            }

            myRelease(con, debug);
        } catch (Exception e) {
            TranImpl.rollbackAndThrow(con, e, getDebug());
        }
    }

    public void validateTable() throws DAOException {
        PrintWriter debug = connectionPool.getDebugWriter();
        Connection con = null;
        try {
            con = connectionPool.getConnection();
            if (debug != null) {
                debug.println("validateTable: " + tableName);
            }
            // So many possible errors, catch DAOExceptions below, close con and then re-throw
            DatabaseMetaData metaData = con.getMetaData();
            ColumnList columnList = new ColumnList(metaData, schemaName, tableNameWithoutSchema);

            if (debug != null) {
                for (Iterator<Column> iter = columnList.iterator(); iter.hasNext();) {
                    debug.println(iter.next().toString());
                }
                for (Property p : properties) {
                    debug.println(p.toString());
                }
            }

            Iterator<Column> columnIter = columnList.iterator();

            String advise = "Database table does not match JavaBean.  "
                    + "Perhaps you changed the bean since the table was created.  "
                    + "The easy fix is to drop the table and let GenericDAO recreate it to match the bean.  ";

            for (Property prop : properties) {
                if (!columnIter.hasNext()) {
                    throw new DAOException(advise + "Table=" + tableName
                            + " is missing column: " + prop.getColumnName()
                            + " that backs " + prop);
                }
                Column column = columnIter.next();
                if (!column.name.equals(prop.getColumnName())) {
                    throw new DAOException(advise + "Table=" + tableName
                            + "Column #" + column.position
                            + " should have name " + prop.getColumnName()
                            + " (but is instead " + column.name + ")");
                }

                if (prop.isPrimaryKeyProperty() && !column.isPrimaryKey) {
                    throw new DAOException(advise + "Table=" + tableName
                            + " does not indicate column \"" + column.name
                            + "\" as part of the primary key (and it should)");
                }

                if (column.isPrimaryKey && !(prop.isPrimaryKeyProperty())) {
                    throw new DAOException(advise + "Table=" + tableName
                            + " does indicates column \"" + column.name
                            + "\" as part of the primary key (and it should not)");
                }

                Class<?> dbType = sqlToJava(column.sqlType);
                if (dbType == null) {
                    throw new DAOException(advise + "Table=" + tableName + ", "
                            + column.name
                            + ": do not know how to map this database type: "
                            + column.sqlType);
                }

                if (dbType != prop.getColumnType()) {
                    throw new DAOException(advise + "Table=" + tableName + ", column="
                            + column.name
                            + ": bean & DB types do not match: beanType="
                            + prop.getColumnType() + ", DBType=" + dbType);
                }

                if (dbType == String.class && column.columnSize != prop.getColumnMaxStrLen()) {
                    throw new DAOException(advise + "Table=" + tableName + ", column=" + column.name
                            + ": bean specifies max string length is " + prop.getColumnMaxStrLen()
                            + (prop.getColumnMaxStrLen() == 255 ? " (which is the default)" : "")
                            + " & DB specifies the max string length is " + column.columnSize);
                }

                if (column.isPrimaryKey) {
                    if (!column.isNonNull) {
                        throw new DAOException(advise + "Table=" + tableName
                                + ", "
                                + column.name
                                + ": database column allows nulls for this type (and should not because it's part of the primary key)");
                    }
                } else if (prop.getDefaultValue() == null && column.isNonNull) {
                    throw new DAOException(advise + "Table=" + tableName
                            + ", "
                            + column.name
                            + ": database column does not allow nulls for this type (and should because of it's type: "
                            + dbType + ")");
                }
            }

            if (columnIter.hasNext()) {
                Column column = columnIter.next();
                throw new DAOException(advise + "Table=" + tableName
                        + "Column (" + column.name
                        + ") without corresponding bean property");
            }

            if (debug != null) {
                debug.println("validateTable: releasing connection: " + con);
            }
            connectionPool.releaseConnection(con);
        } catch (SQLException e) {
            try {
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e2) {
                /* Ignore */ }
            throw new DAOException(e);
        } catch (DAOException e) {
            try {
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e2) {
                /* Ignore */ }
            throw e;
        }
    }

    protected abstract Object createAutoIncrement(Connection con, B b,
            PrintWriter debug) throws SQLException, RollbackException;

    protected abstract Object fetchMinMaxValue(Connection con,
            MatchArgLeafNode arg, String tableName, PrintWriter debug)
            throws SQLException;

    protected abstract String getLikeOperator();

    protected abstract String getLikeIgnoringCaseOperator();

    protected abstract String getNullSafeEqualsOperator();

    protected abstract String getBlobTypeDeclaration();

    protected abstract String getDateTimeTypeDeclaration();

    protected abstract String getDefaultSchemaName();

    protected abstract String getSerialTypeDeclaration();

    protected abstract String getBigSerialTypeDeclaration();

    protected abstract String getVarCharTypeDeclaration(int maxStringLength);

    protected void checkMaxStringLength(Property p, Object value) {
        if (p.getType() != String.class) {
            return;
        }

        if (value == null) {
            return;
        }

        if (!(value instanceof String)) {
            return;
        }

        String str = (String) value;
        if (str.length() <= p.getColumnMaxStrLen()) {
            return;
        }

        throw new IllegalArgumentException("Attempt to set value of String property to value longer than max permitted: "
                + "property = " + p.getName()
                + ", max len = " + p.getColumnMaxStrLen()
                + ", len of value = " + str.length());
    }

    private String concatNameSepSuff(Property[] props, String separator,
            String suffix) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < props.length; i++) {
            if (i > 0) {
                sb.append(separator);
            }
            sb.append(props[i].getColumnName());
        }
        sb.append(suffix);
        return sb.toString();
    }

    private String concatTokenSep(Property[] props, String tokenInPlaceOfName, String separator) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < props.length; i++) {
            if (i > 0) {
                sb.append(separator);
            }
            sb.append(tokenInPlaceOfName);
        }
        return sb.toString();
    }

    private String computeSql(MatchArgTree argTree) {
        StringBuffer sql = new StringBuffer();
        sql.append("SELECT * FROM ");
        sql.append(tableName);

        String whereTest = computeWhereTest(argTree);
        if (whereTest.length() > 0) {
            sql.append(" WHERE ");
            sql.append(whereTest);
        }

        if (TranImpl.isActive()) {
            sql.append(" FOR UPDATE");
        }

        return sql.toString();
    }

    private String computeWhereTest(MatchArgTree argTree) {
        MatchOp op = argTree.getOp();

        if (argTree instanceof MatchArgInternalNode) {
            MatchArgInternalNode internalNode = (MatchArgInternalNode) argTree;
            List<MatchArgTree> subNodes = internalNode.getSubNodes();
            StringBuffer sql = new StringBuffer();
            for (MatchArgTree subNode : subNodes) {
                if (sql.length() > 0) {
                    if (op == MatchOp.AND) {
                        sql.append(" AND ");
                    }
                    if (op == MatchOp.OR) {
                        sql.append(" OR ");
                    }
                }
                sql.append('(');
                sql.append(computeWhereTest(subNode));
                sql.append(')');
            }
            return sql.toString();
        }

        if (op == null) {
            return "NULL is not ?"; // op is null when a max or min constraint
        }                                    // match any rows

        MatchArgLeafNode leaf = (MatchArgLeafNode) argTree;
        String keyName = leaf.getProperty().getName();
        switch (op) {
            case EQUALS:
                return keyName + " " + getNullSafeEqualsOperator() + " ?";
            case NOT_EQUALS:
                return "NOT (" + keyName + " " + getNullSafeEqualsOperator()
                        + " ?)";
            case GREATER:
                return keyName + " > ?";
            case GREATER_OR_EQUALS:
                return keyName + " >= ?";
            case LESS:
                return keyName + " < ?";
            case LESS_OR_EQUALS:
                return keyName + " <= ?";
            case CONTAINS:
            case STARTS_WITH:
            case ENDS_WITH:
                return keyName + " " + getLikeOperator() + " ?";
            case EQUALS_IGNORE_CASE:
            case CONTAINS_IGNORE_CASE:
            case STARTS_WITH_IGNORE_CASE:
            case ENDS_WITH_IGNORE_CASE:
                return keyName + " " + getLikeIgnoringCaseOperator() + " ?";
            case MAX:
            case MIN:
                throw new AssertionError(
                        op
                        + " in constraints should have be converted to EQUALS at this point");
            default:
                throw new AssertionError("Unknown op: " + op);
        }
    }

    private Property[] extractProperties(boolean primaryKey) {
        List<Property> list = new ArrayList<Property>();
        for (Property p : properties) {
            if (p.isPrimaryKeyProperty() == primaryKey) {
                list.add(p);
            }
        }
        return list.toArray(new Property[list.size()]);
    }

    private void fixMaxMin(MatchArgTree argTree, PrintWriter debug)
            throws RollbackException {
        // Max and min matches must be run in a transaction
        if (!TranImpl.isActive()) {
            throw new AssertionError("Caller should have started a transaction");
        }

        Iterator<MatchArgLeafNode> iter = argTree.leafIterator();
        while (iter.hasNext()) {
            MatchArgLeafNode arg = iter.next();
            MatchOp op = arg.getOp();

            if (op == MatchOp.MAX || op == MatchOp.MIN) {
                Connection con = connectionPool.getTransactionConnection();

                try {
                    Object matchValue = fetchMinMaxValue(con, arg, tableName,
                            debug);

                    if (matchValue == null) {
                        // If there is no match from some max or min op, we set
                        // the constraint's op
                        // to null. This causes this constraint to evaluate to
                        // false.
                        arg.fixConstraint(null, null);
                    } else {
                        arg.fixConstraint(MatchOp.EQUALS, matchValue);
                    }
                } catch (SQLException e) {
                    TranImpl.rollbackAndThrow(con, e, getDebug());
                    throw new AssertionError("executeRollback returned");
                }
            }
        }
    }

    private void fixDBValuesForPartialStringMatch(MatchArgTree argTree) {
        Iterator<MatchArgLeafNode> iter = argTree.leafIterator();
        while (iter.hasNext()) {
            MatchArgLeafNode arg = iter.next();

            Object value = arg.getValue();
            if (value instanceof String) {
                String strValue = (String) value;
                MatchOp op = arg.getOp();
                switch (op) {
                    case CONTAINS:
                    case CONTAINS_IGNORE_CASE:
                        arg.fixConstraint(op, '%' + strValue + '%');
                        break;
                    case STARTS_WITH:
                    case STARTS_WITH_IGNORE_CASE:
                        arg.fixConstraint(op, strValue + '%');
                        break;
                    case ENDS_WITH:
                    case ENDS_WITH_IGNORE_CASE:
                        arg.fixConstraint(op, '%' + strValue);
                        break;
                    default:
                    // Do nothing
                }
            }
        }
    }

    private String javaToSql(Class<?> javaType, Property prop,
            int maxStringLength) throws DAOException {
        StringBuffer sql = new StringBuffer();

        // Types that in Java default to NULL
        if (javaType.isEnum()) {
            sql.append(getVarCharTypeDeclaration(maxStringLength));
        }

        if (javaType == String.class) {
            sql.append(getVarCharTypeDeclaration(maxStringLength));
        }

        if (javaType == java.sql.Date.class) {
            sql.append("DATE");
        }
        if (javaType == java.util.Date.class) {
            sql.append(getDateTimeTypeDeclaration());
        }
        if (javaType == java.sql.Time.class) {
            sql.append("TIME");
        }

        if (javaType == byte[].class) {
            sql.append(getBlobTypeDeclaration());
        }

        if (sql.length() > 0) {
            if (prop.isPrimaryKeyProperty()) {
                sql.append(" NOT NULL");
            }
            return sql.toString();
        }

        // Types that in Java default to NOT NULL DEFAULT 0
        if (javaType == boolean.class) {
            sql.append("BOOLEAN");
        }
        if (javaType == double.class) {
            sql.append("DOUBLE PRECISION");
        }
        if (javaType == float.class) {
            sql.append("FLOAT4");
        }
        if (javaType == int.class) {
            sql.append("INT");
        }
        if (javaType == long.class) {
            sql.append("BIGINT");
        }

        if (sql.length() > 0) {
            if (prop.isPrimaryKeyProperty()) {
                sql.append(" NOT NULL");
            } else if (javaType == boolean.class) {
                sql.append(" NOT NULL DEFAULT FALSE");
            } else {
                sql.append(" NOT NULL DEFAULT 0");
            }
            return sql.toString();
        }

        throw new DAOException("Cannot map Java type: "
                + javaType.getCanonicalName());
    }

    private Class<?> sqlToJava(int sqlType) throws DAOException {
        if (sqlType == Types.VARCHAR) {
            return String.class;
        }
        if (sqlType == Types.VARBINARY) {
            return String.class;
        }

        if (sqlType == Types.BIT) {
            return boolean.class;
        }
        if (sqlType == Types.BOOLEAN) {
            return boolean.class;
        }
        if (sqlType == Types.TINYINT) {
            return boolean.class;
        }

        if (sqlType == Types.INTEGER) {
            return int.class;
        }
        if (sqlType == Types.BIGINT) {
            return long.class;
        }

        if (sqlType == Types.DOUBLE) {
            return double.class;
        }
        if (sqlType == Types.REAL) {
            return float.class;
        }

        if (sqlType == Types.DATE) {
            return java.sql.Date.class;
        }
        if (sqlType == Types.TIME) {
            return java.sql.Time.class;
        }
        if (sqlType == Types.TIMESTAMP) {
            return java.util.Date.class;
        }

        if (sqlType == Types.LONGVARBINARY) {
            return byte[].class;
        }
        if (sqlType == Types.BLOB) {
            return byte[].class;
        }
        if (sqlType == Types.BINARY) {
            return byte[].class;
        }

        throw new DAOException("Cannot map SQL type: " + sqlType);
    }

    private B[] sqlMatch(MatchArgTree argTree) throws RollbackException {
        PrintWriter debug = getDebug();
        try {
            if (argTree.containsMaxOrMin()) {
                fixMaxMin(argTree, debug);
            }

            String sql = computeSql(argTree);
            fixDBValuesForPartialStringMatch(argTree);
            return executeQuery(sql, (Object[]) argTree.getValues());
        } catch (Exception e) {
            TranImpl.rollbackAndThrow(e);
            throw new AssertionError("rollbackAndThrow() returned");
        }
    }

    private void validatePrimaryKeyValues(Object[] keyValues)
            throws RollbackException {
        // Note this method validates properties and types of the values, but
        // does not clone them
        // Problems found cause IllegalArgumentException or NullPointerException
        // to be thrown
        // All exceptions (including IllegalArgumentException and
        // NullPointerException) are caught and
        // chained in RollbackException to ensure any active transaction for
        // this thread is rolled back.

        try {
            if (keyValues == null) {
                throw new NullPointerException("keyValues");
            }

            if (primaryKeyProperties.length != keyValues.length) {
                throw new IllegalArgumentException(
                        "Wrong number of key values: " + keyValues.length
                        + " (should be " + primaryKeyProperties.length
                        + ")");
            }

            for (int i = 0; i < primaryKeyProperties.length; i++) {
                if (keyValues[i] == null) {
                    throw new NullPointerException(
                            "Primary key value cannot be null: property="
                            + primaryKeyProperties[i].getName());
                }

                if (!primaryKeyProperties[i].isInstance(keyValues[i])) {
                    throw new IllegalArgumentException(
                            "Key value for property "
                            + primaryKeyProperties[i].getName()
                            + " is not instance of "
                            + primaryKeyProperties[i].getType()
                            + ".  Rather it is "
                            + keyValues[i].getClass());
                }
            }
        } catch (Exception e) {
            TranImpl.rollbackAndThrow(e);
        }
    }

    public static <B> GenericDAOImpl<B> getInstance(Class<B> beanClass, String tableName, ConnectionPool connectionPool)
            throws DAOException {
        String jdbcDriverName = connectionPool.getDriverName();

        if (jdbcDriverName == null) {
            throw new NullPointerException("jdbcDriverName");
        }

        GenericDAOImpl<B> instance = null;

        if (jdbcDriverName.contains("mysql")) {
            instance = new MySQLImpl<B>(beanClass, tableName, connectionPool);
        }
        /*
        * if (jdbcDriverName.contains("postgres")) { instance = new
        * PostgresTable<B>(beanClass,tableName,connectionPool); }
         */
        if (instance == null) {
            throw new DAOException(
                    "JDBC Driver does not appear to be for a supported database (MySQL)");
        }

        return instance;
    }

    private static class Column {

        String name;
        int sqlType;
        int columnSize;
        boolean isNonNull;
        boolean isPrimaryKey;
        int position;

        public String toString() {
            StringBuilder sb = new StringBuilder("Column#");
            sb.append(position).append('(').append(name);
            if (isPrimaryKey) {
                sb.append(", primary key");
            }
            if (isNonNull) {
                sb.append(", non null");
            }
            sb.append(", ").append(sqlType).append(')');
            return sb.toString();
        }
    }

    private static class ColumnList {

        ArrayList<Column> list = new ArrayList<Column>();

        ColumnList(DatabaseMetaData metaData, String schemaName,
                String tableNameWithoutSchema) throws SQLException {
            ResultSet rs = metaData.getColumns(null, schemaName,
                    tableNameWithoutSchema, null);
            int pos = 0;
            while (rs.next()) {
                Column c = new Column();
                c.name = rs.getString("COLUMN_NAME");
                c.sqlType = rs.getInt("DATA_TYPE");
                c.columnSize = rs.getInt("COLUMN_SIZE");
                c.isNonNull = rs.getString("IS_NULLABLE").equals("NO");
                c.isPrimaryKey = false; // Will update below if primary key column
                pos++;
                c.position = pos;
                list.add(c);
            }
            rs.close();

            rs = metaData.getPrimaryKeys(null, schemaName,
                    tableNameWithoutSchema);
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                for (Column col : list) {
                    if (col.name.equals(columnName)) {
                        col.isPrimaryKey = true;
                    }
                }
            }
            rs.close();

            Collections.sort(list, new Comparator<Column>() {
                public int compare(Column c1, Column c2) {
                    return c1.position - c2.position;
                }
            });
        }

        Iterator<Column> iterator() {
            return list.iterator();
        }
    }
}
