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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The annotation used declare the primary key properties of a Java Bean.
 * <p>
 * The names of the primary key properties are passed as a String.  You must
 * use Java capitalization conventions.
 * <p>
 * Here is a typical example:
 * <blockquote><pre>
 *     &#64;PrimaryKey("id")
 *     public class Customer {
 *         private int    id;
 *         private String firstName;
 *         private String lastName;
 *
 *         public int     getId()        { return id;        }
 *         public String  getFirstName() { return firstName; }
 *         public String  getLastName()  { return lastName;  }
 *
 *         public void setId(int i)           { id = i; }
 *         public void setFirstName(String s) { firstName = s; }
 *         public void setLastName(String s)  { lastName = s;  }
 *     }
 * </pre></blockquote>
 * <p>
 * In the typical case, as shown above, <code>id</code> is the primary key.
 * Whenever the primary key is an <code>int</code> or a <code>long</code>,
 * <code>GenericDAO</code> will create an auto-increment field to store the
 * key and <code>GenericDAO</code> will let the database generate the value
 * when rows are created (using <code>create()</code>). 
 * <p>
 * Properties of composite keys must be listed in order, comma separated:
 * <blockquote><pre>
 *     &#64;PrimaryKey("lastName,firstName")
 *     public class AddressBookEntry {
 *         private String firstName;
 *         private String lastName;
 *         ...
 *     }
 * </pre></blockquote>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface PrimaryKey {
	String value();
}
