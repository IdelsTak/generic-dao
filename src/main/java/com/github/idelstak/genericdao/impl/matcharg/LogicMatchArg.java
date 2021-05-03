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

public class LogicMatchArg extends MatchArg {
	private MatchOp    op;
	private MatchArg[] constraints;

    public LogicMatchArg(MatchOp op, MatchArg...constraints) {
    	this.op = op;
    	this.constraints = constraints.clone();
    }

    public MatchArg[] getArgs() { return constraints.clone(); }
    public MatchOp    getOp()   { return op;                  }
}
