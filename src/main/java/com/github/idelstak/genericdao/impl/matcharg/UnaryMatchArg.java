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

public class UnaryMatchArg extends MatchArg {

    private String keyName;
    private MatchOp op;

    public UnaryMatchArg(String keyName, MatchOp op) {
        this.keyName = keyName;
        this.op = op;
    }

    public String getKeyName() {
        return keyName;
    }

    public MatchOp getOp() {
        return op;
    }
}
