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

import com.github.idelstak.genericdao.impl.matcharg.BinaryMatchArg;
import com.github.idelstak.genericdao.impl.matcharg.LogicMatchArg;
import com.github.idelstak.genericdao.impl.matcharg.MatchOp;
import com.github.idelstak.genericdao.impl.matcharg.UnaryMatchArg;

/**
 * A class to specify constraints when matching beans. Use with the
 * <tt>GenericDAO.match()</tt> method, which generates a SQL SELECT call on the
 * underlying table. The <tt>MatchArg</tt> parameters are converted into the
 * WHERE clause for the SELECT so as to constrain which rows are returned.
 * <p>
 * For example, given a the following <tt>User</tt> bean:
 * <blockquote>
 * 
 * <pre>
 * public class User {
 *     private String userName;
 *     private String password;
 *     private String firstName;
 *     private String lastName;
 * 
 *     public String getUserName() {
 *         return userName;
 *     }
 * 
 *     public String getPassword() {
 *         return password;
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
 *     public void setUserName(String s) {
 *         userName = s;
 *     }
 * 
 *     public void setPassword(String s) {
 *         password = s;
 *     }
 * 
 *     public void setFirstName(String s) {
 *         firstName = s;
 *     }
 * 
 *     public void setLastName(String s) {
 *         lastName = s;
 *     }
 * }
 * </pre>
 * 
 * </blockquote>
 * <p>
 * this call would return all users with password equal to &quot;testing&quot;.:
 * <blockquote>
 * 
 * <pre>
 * User[] array = dao.match(MatchArg.equals(&quot;password&quot;, &quot;testing&quot;));
 * </pre>
 * 
 * </blockquote>
 *
 */
public abstract class MatchArg {
    protected MatchArg() {
    }

    protected abstract MatchOp getOp();

    /**
     * Logical AND operator for use with the <tt>GenericDAO.match()</tt> method.
     * Takes as parameters a variable number of MatchArg constraints, all of
     * which must evaluate to true for a row to be returned by
     * <tt>GenericDAO.match()</tt>. For example, using the <tt>User</tt> bean
     * defined above, this match call would return all users with first name
     * &quot;George&quot; and last name &quot;Bush&quot;.: <blockquote>
     * 
     * <pre>
     * User[] array = dao.match(MatchArg.and(MatchArg.equals(&quot;firstName&quot;, &quot;George&quot;), MatchArg.equals(&quot;lastName&quot;, &quot;Bush&quot;)));
     * </pre>
     * 
     * </blockquote>
     * 
     * @param constraints
     *            zero or more other <tt>MatchArg</tt> parameters
     * @return a <tt>MatchArg</tt> which evaluates true for a row if all the
     *         constraint arguments evaluate to true for that row
     */
    public static MatchArg and(MatchArg... constraints) {
        return new LogicMatchArg(MatchOp.AND, constraints);
    }

    /**
     * String &quot;contains&quot; operator for use with the
     * <tt>GenericDAO.match()</tt> method. It evaluates to true for a row when
     * the value of the specified field contains the given string value. The
     * database field must contain string values. If it does not, an exception
     * is thrown by the <tt>match()</tt> method.
     * <p>
     * For example, using the <tt>User</tt> bean defined above, this match would
     * return all users whose first name contains a lower case double
     * &quot;r&quot;: <blockquote>
     * 
     * <pre>
     * User[] array = dao.match(MatchArg.contains(&quot;firstName&quot;, &quot;rr&quot;));
     * </pre>
     * 
     * </blockquote>
     * 
     * @param fieldName
     *            the name of the field in the table being matched. Field must
     *            contain string values.
     * @param s
     *            the substring which must be present in the specified field for
     *            this <tt>MatchArg</tt> to evaluate to true.
     * @return a <tt>MatchArg</tt> which evaluates true for a row if the
     *         specified field contains the given string
     */
    public static MatchArg contains(String fieldName, String s) {
        return new BinaryMatchArg(fieldName, MatchOp.CONTAINS, s);
    }

    /**
     * String &quot;contains&quot; operator for use with the
     * <tt>GenericDAO.match()</tt> method. It evaluates to true for a row when
     * the value of the specified field contains the given string value,
     * ignoring case. The database field must contain string values. If it does
     * not, an exception is thrown by the <tt>match()</tt> method.
     * <p>
     * For example, using the <tt>User</tt> bean defined above, this match would
     * return all users whose first name contains the substring &quot;st&quot;,
     * &quot;sT&quot;, &quot;St&quot;, or &quot;ST&quot;: <blockquote>
     * 
     * <pre>
     * User[] array = dao.match(MatchArg.containsIgnoreCase(&quot;firstName&quot;, &quot;st&quot;));
     * </pre>
     * 
     * </blockquote>
     * 
     * @param fieldName
     *            the name of the field in the table being matched. Field must
     *            contain string values.
     * @param s
     *            the substring which must be present in the specified field,
     *            ignoring case, for this <tt>MatchArg</tt> to evaluate to true.
     * @return a <tt>MatchArg</tt> which evaluates true for a row if the
     *         specified field contains the given string, ignoring case.
     */
    public static MatchArg containsIgnoreCase(String fieldName, String s) {
        return new BinaryMatchArg(fieldName, MatchOp.CONTAINS_IGNORE_CASE, s);
    }

    /**
     * String &quot;ends with&quot; operator for use with the
     * <tt>GenericDAO.match()</tt> method. It evaluates to true for a row when
     * the value of the specified field ends with the given string value. The
     * database field must contain string values. If it does not, an exception
     * is thrown by the <tt>match()</tt> method.
     * <p>
     * For example, using the <tt>User</tt> bean defined above, this match would
     * return all users whose first name ends with the lower case &quot;st&quot;
     * characters: <blockquote>
     * 
     * <pre>
     * User[] array = dao.match(MatchArg.endsWith(&quot;firstName&quot;, &quot;st&quot;));
     * </pre>
     * 
     * </blockquote>
     * 
     * @param fieldName
     *            the name of the field in the table being matched. Field must
     *            contain string values.
     * @param ending
     *            the substring which must be present at the end of the
     *            specified field for this <tt>MatchArg</tt> to evaluate to
     *            true.
     * @return a <tt>MatchArg</tt> which evaluates true for a row if the
     *         specified field ends with the given string
     */
    public static MatchArg endsWith(String fieldName, String ending) {
        return new BinaryMatchArg(fieldName, MatchOp.ENDS_WITH, ending);
    }

    /**
     * String &quot;ends with&quot; operator for use with the
     * <tt>GenericDAO.match()</tt> method. It evaluates to true for a row when
     * the value of the specified field ends with the given string value,
     * ignoring case. The database field must contain string values. If it does
     * not, an exception is thrown by the <tt>match()</tt> method.
     * <p>
     * For example, using the <tt>User</tt> bean defined above, this match would
     * return all users whose first name contains the &quot;st&quot;,
     * &quot;sT&quot;, &quot;St&quot;, or &quot;ST&quot;: <blockquote>
     * 
     * <pre>
     * User[] array = dao.match(MatchArg.endsWithIgnoreCase(&quot;firstName&quot;, &quot;st&quot;));
     * </pre>
     * 
     * </blockquote>
     * 
     * @param fieldName
     *            the name of the field in the table being matched. Field must
     *            contain string values.
     * @param ending
     *            the substring which must be present at the end of the
     *            specified field, ignoring case, for this <tt>MatchArg</tt> to
     *            evaluate to true.
     * @return a <tt>MatchArg</tt> which evaluates true for a row if the
     *         specified field ends with the given string, ignoring case.
     */
    public static MatchArg endsWithIgnoreCase(String fieldName, String ending) {
        return new BinaryMatchArg(fieldName, MatchOp.ENDS_WITH_IGNORE_CASE, ending);
    }

    /**
     * Equals operator for use with the <tt>GenericDAO.match()</tt> method. It
     * evaluates to true for a row when the value of the specified field equals
     * the given value. This operator can be used on all types, except array
     * types. Except, it can be used with <tt>byte[]</tt>. The
     * <tt>matchValue</tt> must be of a type appropriate for the specified
     * field. This is checked by the <tt>match()</tt> method.
     * <p>
     * For example, using the <tt>User</tt> bean defined above, this match would
     * return all users whose friend count is zero: <blockquote>
     * 
     * <pre>
     * User[] array = dao.match(MatchArg.equals(&quot;friendCount&quot;, 0));
     * </pre>
     * 
     * </blockquote>
     * 
     * @param fieldName
     *            the name of the field being matched.
     * @param matchValue
     *            the value which the specified field must equal for this
     *            <tt>MatchArg</tt> to evaluate to true.
     * @return a <tt>MatchArg</tt> which evaluates true for a row if the
     *         specified field is equals to the given value.
     */
    public static MatchArg equals(String fieldName, Object matchValue) {
        return new BinaryMatchArg(fieldName, MatchOp.EQUALS, matchValue);
    }

    public static MatchArg equalsIgnoreCase(String keyName, String matchValue) {
        return new BinaryMatchArg(keyName, MatchOp.EQUALS_IGNORE_CASE, matchValue);
    }

    public static MatchArg greaterThan(String keyName, Object matchValue) {
        return new BinaryMatchArg(keyName, MatchOp.GREATER, matchValue);
    }

    public static MatchArg greaterThanOrEqualTo(String keyName, Object matchValue) {
        return new BinaryMatchArg(keyName, MatchOp.GREATER_OR_EQUALS, matchValue);
    }

    public static MatchArg lessThan(String keyName, Object matchValue) {
        return new BinaryMatchArg(keyName, MatchOp.LESS, matchValue);
    }

    public static MatchArg lessThanOrEqualTo(String keyName, Object matchValue) {
        return new BinaryMatchArg(keyName, MatchOp.LESS_OR_EQUALS, matchValue);
    }

    public static MatchArg max(String keyName) {
        return new UnaryMatchArg(keyName, MatchOp.MAX);
    }

    public static MatchArg min(String keyName) {
        return new UnaryMatchArg(keyName, MatchOp.MIN);
    }

    /**
     * Not equals operator for use with the <tt>GenericDAO.match()</tt> method.
     * It evaluates to true for a row when the value of the specified field is
     * not equal to the given value. This operator can be used on all types,
     * except array types. Except, it can be used with <tt>byte[]</tt>. The
     * <tt>matchValue</tt> must be of a type appropriate for the specified
     * field. This is checked by the <tt>match()</tt> method.
     * <p>
     * For example, using the <tt>User</tt> bean defined above, this match would
     * return all users whose friend count not zero: <blockquote>
     * 
     * <pre>
     * User[] array = dao.match(MatchArg.notEquals(&quot;friendCount&quot;, 0));
     * </pre>
     * 
     * </blockquote>
     * 
     * @param fieldName
     *            the name of the field being matched.
     * @param matchValue
     *            the value which the specified field must not equal for this
     *            <tt>MatchArg</tt> to evaluate to true.
     * @return a <tt>MatchArg</tt> which evaluates true for a row if the
     *         specified field is not equal to the given value.
     */
    public static MatchArg notEquals(String fieldName, Object matchValue) {
        return new BinaryMatchArg(fieldName, MatchOp.NOT_EQUALS, matchValue);
    }

    /**
     * Logical OR operator for use with the <tt>GenericDAO.match()</tt> method.
     * Takes as parameters a variable number of MatchArgs, any one of which must
     * evaluate to true for a row to be returned. For example, using the
     * <tt>User</tt> bean defined above, this match would return all users with
     * first name &quot;William&quot; or first name &quot;Bill&quot;.:
     * <blockquote>
     * 
     * <pre>
     *     User[] array = dao.match(
     *                          MatchArg.or(
     *                                MatchArg.equals("firstName", "William"),
     *                                MatchArg.equals("firstName", "Bill"));
     * </pre>
     * 
     * </blockquote>
     * 
     * @param constraints
     *            zero or more other <tt>MatchArg</tt> parameters
     * @return true when matching rows if all the constraint arguments evaluate
     *         to true for for that row
     */
    public static MatchArg or(MatchArg... constraints) {
        return new LogicMatchArg(MatchOp.OR, constraints);
    }

    public static MatchArg startsWith(String keyName, String beginning) {
        return new BinaryMatchArg(keyName, MatchOp.STARTS_WITH, beginning);
    }

    public static MatchArg startsWithIgnoreCase(String keyName, String beginning) {
        return new BinaryMatchArg(keyName, MatchOp.STARTS_WITH_IGNORE_CASE, beginning);
    }
}
