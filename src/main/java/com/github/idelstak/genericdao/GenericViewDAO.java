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

import com.github.idelstak.genericdao.impl.GenericViewDAOImpl;

/**
 * This class is used to read rows of a database table and return them in
 * instances of a JavaBean of type <tt>B</tt>. Usually, the table is a view
 * created by executing a SQL Join. You provide the SQL, this class creates the
 * JavaBeans. The fields returned must have the same names as the properties of
 * the JavaBean.
 *
 * (The GenericDAO class uses the internal implementation of this class to
 * implement its <tt>match()</tt> and <tt>read()</tt> methods.)
 * <p>
 * Here is an example of a bean representing a student:
 * <blockquote>
 *
 * <pre>
 * public class Student {
 *     private String studentId;
 *     private String firstName;
 *     private String lastName;
 *     private double gpa;
 *
 *     public String getStudentId() {
 *         return studentId;
 *     }
 *
 *     public String getFirstName() {
 *         return firstName;
 *     }
 *
 *     public String getLastName() {
 *         return lastName;
 *     }
 *
 *     public String getGpa() {
 *         return gpa;
 *     }
 *
 *     public void setStudentId(String s) {
 *         studentId = s;
 *     }
 *
 *     public void setFirstName(String s) {
 *         firstName = s;
 *     }
 *
 *     public void setLastName(String s) {
 *         lastName = s;
 *     }
 *
 *     public void setGpa(double d) {
 *         gpa = d;
 *     }
 * }
 * </pre>
 *
 * </blockquote>
 * <p>
 * To use this class, instantiate it in the usual way and then call
 * <tt>executeQuery()</tt>. Provide a SQL query that produces a table that
 * contains a field for every property of the <tt>StudentId</tt> JavaBean. To
 * prevent injection attacks, pass user data as separate parameters. Here are
 * some examples:
 * <blockquote>
 *
 * <pre>
 * String sql = &quot;select studentId, firstName, lastName, gpa from student&quot;;
 * StudentId[] a = executeQuery(sql);
 *
 * String sql = &quot;select studentId, firstName, lastName, gpa from student where firstName='Fred'&quot;;
 * StudentId[] a = executeQuery(sql);
 *
 * String sql = &quot;select studentId, firstName, lastName, gpa from student where firstName=?&quot;;
 * StudentId[] a = executeQuery(sql, request.getParameter(&quot;firstName&quot;));
 *
 * String sql = &quot;select student.id as studentId, student.first as firstName, &quot;
 *         + &quot;student.last as lastName, grades.average as gpa &quot;
 *         + &quot;from student, grades&quot;;
 * StudentId[] a = executeQuery(sql);
 *
 * String sql = &quot;select student.id as studentId, student.first as firstName, &quot;
 *         + &quot;student.last as lastName, grades.average as gpa &quot;
 *         + &quot;from student, grades where student.id=?&quot;;
 * StudentId[] a = executeQuery(sql, request.getParameter(&quot;id&quot;));
 * Student s = null;
 * if (a.length != 0) {
 *     s = a[0];
 * }
 * </pre>
 *
 * </blockquote>
 */
public class GenericViewDAO<B> {
    private GenericViewDAOImpl<B> impl;

    /**
     * Constructor
     * @param beanClass
     *            the class description of the bean.
     * @param connectionPool
     *            the connection pool to use to manage connections to the
     *            database.
     * @throws DAOException
     *             if there are any problems, including problems accessing the
     *             database, problems with the bean class, etc.
     */
    public GenericViewDAO(Class<B> beanClass, ConnectionPool connectionPool)
            throws DAOException {
        impl = new GenericViewDAOImpl<B>(beanClass, connectionPool);
    }

    /**
     * Runs a query using the provided SQL and parameters.
     * @param sql
     *          a SQL query, as would be passed to a <tt>PreparedStatement</tt>.
     * @param args
     *          arguments for the SQL query (if any).
     * @return an array of beans that contain the data in the rows returned by
     *          the SQL query.  If no rows are returned by the query,
     *          a zero length array is returned.
     *          (This method never returns <code>null</code>.)
     * @throws RollbackException
     *          if there are errors in the types of the arguments, or
     *          if there is an error accessing the database, including
     *          SQLException or deadlock.
     */
    public B[] executeQuery(String sql, Object... args)
            throws RollbackException {
        return impl.executeQuery(sql, args);
    }

    public String[] getPropertyNames() {
        return impl.getPropertyNames();
    }
}
