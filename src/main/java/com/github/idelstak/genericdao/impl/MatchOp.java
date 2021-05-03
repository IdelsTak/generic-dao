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

public enum MatchOp {
    // Valid for comparing any types, except arrays (byte[] is okay)
        EQUALS,
        NOT_EQUALS,

    // Valid for comparing numbers, Dates, or Strings
        GREATER,
        GREATER_OR_EQUALS,
        LESS,
        LESS_OR_EQUALS,

    // Valid for matching String properties, only
        CONTAINS,
        STARTS_WITH,
        ENDS_WITH,
        EQUALS_IGNORE_CASE,
        CONTAINS_IGNORE_CASE,
        STARTS_WITH_IGNORE_CASE,
        ENDS_WITH_IGNORE_CASE,

    // Valid for matching the max/min values of numbers, Dates or Strings
        MAX,
        MIN,

    // Logical ops valid only for combining other ops
        OR,
        AND;


    public String toString() {
    	return getClass().getSimpleName()+"."+super.toString();
    }
}
