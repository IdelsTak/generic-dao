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
 * The annotation used specify the maximum length of a String property stored in the database.
 * If this annotation is not provided, the default maximum string that be stored for is 255 characters.
 * If used, this annotation must be placed on the setter for the property.
 * <p>
 * Here is a simple example:
 * <blockquote><pre>
 *     &#64;PrimaryKey("id")
 *     public class Customer {
 *         private int    id;
 *         private String firstName;
 *         private String lastName;
 *         private String comment;
 *
 *         public int     getId()        { return id;        }
 *         public String  getFirstName() { return firstName; }
 *         public String  getLastName()  { return lastName;  }
 *         public String  getComment()   { return comment;   }
 *
 *         public void setId(int i)           { id = i; }
 *
 *         &#64;MaxSize(20)
 *         public void setFirstName(String s) { firstName = s; }
 *
 *         &#64;MaxSize(30)
 *         public void setLastName(String s)  { lastName  = s; }
 *
 *         &#64;MaxSize(1000)
 *         public void setComment(String s)   { comment   = s; }
 *     }
 * </pre></blockquote>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface MaxSize {
    int value();
}
