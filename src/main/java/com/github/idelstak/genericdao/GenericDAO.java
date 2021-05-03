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

import com.github.idelstak.genericdao.impl.GenericDAOImpl;

/**
 * This class is used to read and write rows of a database table that correspond to instances of a JavaBean of type <code>B</code>.
 *
 * Many implementations are possible. Included with this package is an implementation that uses the MySQL database.
 * <p>
 * The JavaBean must either have a public, no-argument constructor
 * <p>
 * JavaBeans are declared in the usual way with getter and setter methods for the properties. The <code>GenericDAO</code> uses the Java reflection
 * classes to inspect the bean. The <code>GenericDAO</code> will only store and retrieve properties that have matching a getter/setter pair. Java
 * capitalization rules must be followed. The getter must have a signature of
 * <blockquote>
 *
 * <pre>
 *     public &lt;type&gt; get&lt;Name&gt;()
 * </pre>
 *
 * </blockquote>
 * <p>
 * and the setter must have a signature of
 * <blockquote>
 *
 * <pre>
 *     public void set&lt;Name&gt;(&lt;type&gt; newValue)
 * </pre>
 *
 * </blockquote>
 * <p>
 * The properties that comprise the primary key (for the table that backs the beans) must be specified with the <code>@PrimaryKey</code> annotation.
 * <p>
 * Here is an example of a simple bean representing a user and its password:
 * <blockquote>
 *
 * <pre>
 * &#064;PrimaryKey(&quot;userName&quot;)
 * public class User {
 *     private String userName;
 *     private String password = null;
 *
 *     public String getUserName() {
 *         return userName;
 *     }
 *
 *     public String getPassword() {
 *         return password;
 *     }
 *
 *     public void setUserName(String s) {
 *         userName = s;
 *     }
 *
 *     public void setPassword(String s) {
 *         password = s;
 *     }
 * }
 * </pre>
 *
 * </blockquote>
 * <p>
 * JavaBean properties can be any of the following Java types: <code>boolean</code>, <code>byte[]</code>, <code>double</code>, <code>float</code>,
 * <code>int</code>, <code>long</code>, <code>java.lang.String</code>, <code>java.sql.Date</code>, <code>java.sql.Time</code>, and
 * <code>java.util.Date</code>.
 * <p>
 * Arrays of the above types (except for arrays of <code>byte[]</code>) are also supported. Multi-dimensional arrays are not allowed. Also, arrays
 * cannot be used as primary keys. In some implementations (such a relational database implementations) auxiliary tables are used for backing arrays.
 * <p>
 * Each table that backs a bean will be indexed by a primary key. The primary key must correspond to one or more of the JavaBean's properties. Primary
 * key properties can be of any of the above types, except for arrays.
 * <p>
 * The <code>GenericDAO</code> will create the table in the database. If the table already exists (presumably because the <code>GenericDAO</code>
 * previously created it), the <code>GenericDAO</code> will compare the table's fields with the bean's properties. If the do not match, an exception
 * is thrown. (See the constructors.) The simplest fix for a table that does not match the bean is to drop the existing table. The
 * <code>GenericDAO</code> will create a new table next time it is instantiated. (Mismatch usually occurs either because there is some old table with
 * the same name that happens to exist or because you've changed the bean since the <code>GenericDAO</code> last created the table. If the latter is
 * the case, you can preserve the old data by using the old version of the bean to read in the old values and the new version of the bean to write
 * them out into a new table.)
 * <p>
 * <code>GenericDAO</code> methods can be invoked from within an enclosing <code>Transaction</code> which will enforce the ACID properties. Specific
 * ACID guarantees provided are particular to the <code>GenericDAO</code> implementation, but typically are just what is implemented by the underlying
 * database. To have the ACID properties of a transaction work across multiple DAOs, you need to be using the same connection pool in each DAO.
 * <p>
 * All <code>GenericDAO</code> calls may be made outside a transaction. In all cases this is equivalent to making the same call from within a
 * transaction and then immediately committing it. However, in many implementations of <code>GenericDAO</code> this done more efficiently. For
 * example, a call outside a transaction to <code>dao.read(userName)</code> is equivalent to:
 * <blockquote>
 *
 * <pre>
 * Transaction.begin();
 * User u = dao.read(userName);
 * Transaction.commit();
 * return u;
 * </pre>
 *
 * </blockquote>
 * <p>
 * All <code>GenericDAO</code> methods (except the constructor methods) throw <code>RollbackException</code> in case of failure. No other exceptions
 * are thrown from <code>GenericDAO</code> methods (other than constructor methods). Any internally thrown exceptions are caught and a
 * <code>RollbackException</code> is thrown with the internal exception as the <code>RollbackException</code>'s cause. Any enclosing transaction is
 * rolled back in the process of throwing <code>RollbackException</code>.
 * <p>
 * All transactions should be committed or rolled back before awaiting user input. If transactions are left running, locks may be held in the
 * underlying database causing new transactions to timeout. To guard against accidentally not committing or rolling back a transaction (perhaps
 * because of a programming bug or an unexpected exception in non-<code>GenericDAO</code> code) you should always put the transaction in a
 * <code>try</code> / <code>catch</code> statement with a <code>finally</code> clause that commits or rolls back the transaction. For example:
 * <blockquote>
 *
 * <pre>
 *     try {
 *         Transaction.begin();
 *         ...
 *         Transaction.commit();
 *     } catch (RollbackException e) {
 *         ...
 *     } finally {
 *         if (Transaction.isActive()) Transaction.rollback();
 *     }
 * </pre>
 *
 * </blockquote>
 * <p>
 * When debugging, it may be helpful to see the SQL that is being generated. This feature is enabled via the <code>ConnectionPool</code> so that it
 * will apply to all DAOs and transactions that may be working together. For example:
 * <blockquote>
 *
 * <pre>
 *      ConnectionPool cp = new ConnectionPool(...);
 *      cp.setDebugOutput(System.out);
 * </pre>
 *
 * </blockquote>
 * <p>
 *
 * @author Jeffrey Eppinger
 * @see com.github.idelstak.genericdao.Transaction
 * @see com.github.idelstak.genericdao.ConnectionPool
 */
public class GenericDAO<B> {

    private final GenericDAOImpl<B> impl;

    /**
     * Instantiates a new <code>GenericDAO</code> object.
     *
     * The constructor will (1) analyze your bean (to make sure it has all the parts it needs to read and write it), (2) create the table in MySQL (if
     * it's not there already), (3) compare the table to the bean (if it already there).
     *
     * @param beanClass the class description of the bean.
     * @param tableName the name of the table in the database used to store instance of the bean.
     * @param connectionPool the connection pool to use to manage connections to the database.
     * @throws DAOException if there are any problems, including problems accessing the database, problems with the bean class, etc.
     */
    public GenericDAO(Class<B> beanClass, String tableName, ConnectionPool connectionPool)
            throws DAOException {
        impl = GenericDAOImpl.getInstance(beanClass, tableName, connectionPool);
        if (!impl.tableExists()) {
            impl.createTable();
        }
        impl.validateTable();
    }

    /**
     * Computes a message digest (a one-way hash) of all the properties for the given bean. The digest is returned as a String. Two beans with the
     * same property values with have digest strings with the same value. This method does not access the database.
     * <p>
     * Digest strings returned from this method are not meant for long-term storage. Future releases of the BeanFactory may compute the digest and/or
     * encode the digest into a string using different algorithms. The current implementation uses the SHA1 algorithm to compute the digest and
     * returns it as a hex string.
     * <p>
     * The primary use of this method is to easily compute a digest for a bean so as to detect that some other thread/user has changed the bean
     * between this thread/user's transactions.
     *
     * @param bean the bean from which to extract values for the digest.
     * @return a string representation of a message digest of the beans properties.
     * @throws RollbackException if the digest cannot be computed for any one of a number of reasons, including errors accessing the bean, etc. Any
     * enclosing transaction is rolled back in the process of throwing this exception.
     */
    public String computeDigest(B bean) throws RollbackException {
        return impl.computeDigest(bean);
    }

    /**
     * Creates a new row in the table using the values provided the contents of the <code>bean</code>.
     *
     * If the primary key for <code>B</code> is either of type <code>int</code> or <code>long</code> (and not a composite key), the primary key field
     * will be auto-increment. Side-effect Warning: In the auto-increment case, the primary key value generated by the database is stored in the
     * <code>bean</code>. (The original value is not read, just overwritten.)
     *
     * If a transaction is active for the current thread when this method is called, the row will be created as part of that existing transaction. (If
     * no transaction is active, this method may create an internal transaction to do the work but will commit that internal transaction before
     * returning.)
     *
     * @param bean an instance of type <code>B</code> that contains the values to store in the table.
     * @throws RollbackException if the work cannot be completed for any one of a number of reasons, including SQLExceptions, deadlocks, errors
     * accessing the bean, etc. Any enclosing transaction is rolled back in the process of throwing this exception.
     * @throws DuplicateKeyException if the <code>bean</code> specifies the creation of a new row with a primary key value that is already in use.
     * This is a (subclass of) <code>RollbackException</code>, so any enclosing transaction is rolled back in the process of throwing this exception.
     */
    public void create(B bean) throws RollbackException {
        impl.create(bean);
    }

    /**
     * Deletes from the table the row with the given primary key.
     *
     * If a transaction is active for the current thread when this method is called, the row will be deleted as part of that existing transaction. (If
     * no transaction is active, this method may create an internal transaction to do the work but will commit that internal transaction before
     * returning.)
     *
     * @param primaryKeyValues the values of the properties that comprise the primary key for bean being deleted.
     * @throws RollbackException if there are errors in the types of the arguments, or if there is no bean in the table with this primary key, or if
     * there is an error accessing the database, including IOException or deadlock. (To avoid getting a RollbackException deleting a bean that doesn't
     * exist in the table, first check to see if it's there using the <code>read()</code> method, inside a transaction.)
     */
    public void delete(Object... primaryKeyValues) throws RollbackException {
        impl.delete(primaryKeyValues);
    }

    /**
     * Returns the number of rows in the table.
     *
     * If a transaction is active for the current thread when this method is called, the whole table will be locked until the active transaction is
     * committed.
     *
     * @return the number of rows in the table.
     *
     * @throws RollbackException if there is an error accessing the database, including SQLException or deadlock.
     */
    public int getCount() throws RollbackException {
        return impl.getBeanCount();
    }

    /**
     * Searches the table for rows matching the given constraints. Constraints are specified with <code>MatchArg</code>s which limit properties to
     * values or ranges, such as equals, less-than or greater-than a given value. Operators on strings also include starts-with, ends-with, and
     * contains. A bean is instantiated and returned to hold the values for each row that matches the constraints.
     *
     * If no constraints are specified, all the rows in the table are returned.
     *
     * If there is an enclosing transaction active on this thread when this method is called, the rows in the table that match be locked by the
     * transaction preventing other transactions from reading or writing these rows until the enclosing transaction is committed or rolled back.
     *
     * @param constraints zero or more constraints, all of which must be <code>true</code> for each bean returned by this call.
     * @return an array of beans that match the given constraints. If no beans match the constraints, a zero length array is returned. (This method
     * never returns <code>null</code>.)
     * @throws RollbackException if there are errors in the types of the arguments, or if there is an error accessing the database, including
     * SQLException or deadlock.
     */
    public B[] match(MatchArg... constraints) throws RollbackException {
        return impl.match(constraints);
    }

    /**
     * Returns the row in the table with the given primary key.
     *
     * If there is an enclosing transaction active on this thread when this method is called, the row in the table will be locked by the transaction
     * preventing other transactions from reading or writing this row until the enclosing transaction is committed or rolled back.
     *
     * @param primaryKeyValues the values of the properties that comprise the primary key for bean being looked up.
     * @return a reference to an instance of <code>B</code> with the given primary key and values populated from the table. If there is no such row,
     * then <code>null</code> is returned.
     * @throws RollbackException if there are errors in the types of the arguments, or if there is an error accessing the database, including
     * SQLException or deadlock.
     */
    public B read(Object... primaryKeyValues) throws RollbackException {
        return impl.read(primaryKeyValues);
    }

    /**
     * Updates the row in the table with the primary key specified by the values in the <code>bean</code> passed in as a parameter. The fields in the
     * row (other than the primary key fields) are set to the values specified in the <code>bean</code>.
     *
     * If a transaction is active for the current thread when this method is called, the row will be updated as part of that existing transaction. (If
     * no transaction is active, this method may create an internal transaction to do the work but will commit that internal transaction before
     * returning.)
     *
     * @param bean an instance of type <code>B</code> that contains the values to store in the table.
     * @throws RollbackException if there is an error accessing the database, including SQLException or deadlock.
     */
    public void update(B bean) throws RollbackException {
        impl.update(bean);
    }
}
