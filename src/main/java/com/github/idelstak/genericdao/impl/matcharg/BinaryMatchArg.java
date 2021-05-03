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
package com.github.idelstak.genericdao.impl.matcharg;

import com.github.idelstak.genericdao.MatchArg;

public class BinaryMatchArg extends MatchArg {
    private String  fieldName;
    private MatchOp op;
    private Object  fieldValue;

    public BinaryMatchArg(String fieldName, MatchOp op, Object fieldValue) {
        this.fieldName  = fieldName;
        this.op         = op;
        this.fieldValue = fieldValue;
    }

    public String  getFieldName()  { return fieldName;  }
    public MatchOp getOp()         { return op;         }
    public Object  getFieldValue() { return fieldValue; }
}
