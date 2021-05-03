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

import java.util.Iterator;

import com.github.idelstak.genericdao.MatchArg;
import com.github.idelstak.genericdao.RollbackException;
import com.github.idelstak.genericdao.impl.matcharg.BinaryMatchArg;
import com.github.idelstak.genericdao.impl.matcharg.LogicMatchArg;
import com.github.idelstak.genericdao.impl.matcharg.MatchArgInternalNode;
import com.github.idelstak.genericdao.impl.matcharg.MatchArgLeafNode;
import com.github.idelstak.genericdao.impl.matcharg.MatchOp;
import com.github.idelstak.genericdao.impl.matcharg.UnaryMatchArg;

public abstract class MatchArgTree {

	/*
     * Note this method validates properties and types of the values, but does not clone them
     * Problems found cause IllegalArgumentException or NullPointerException to be thrown.
     * All exceptions (including IllegalArgumentException and NullPointerException) are caught and
     * chained in RollbackException to ensure any active transaction for this thread is rolled back.
     *
     * Checks for the following:
     *     No array properties (but byte[] is not checked as it's allowed for EQUALS)
     *
     *     For binary operators:
     *         * no null match values for primary key properties
     *         * match value must be of same type as property
     *         * no null values for non-nullable fields
     *
     */

	public static MatchArgTree buildTree(Property[] allBeanProperties, MatchArg constraint) throws RollbackException {
        try {
            if (constraint == null) throw new NullPointerException("constraint cannot be null)");

        	if (constraint instanceof UnaryMatchArg) {
        		UnaryMatchArg arg = (UnaryMatchArg) constraint;
        		return new MatchArgLeafNode(allBeanProperties,arg);
        	}

        	if (constraint instanceof BinaryMatchArg) {
        		BinaryMatchArg arg = (BinaryMatchArg) constraint;
        		return new MatchArgLeafNode(allBeanProperties,arg);
        	}

    		LogicMatchArg arg = (LogicMatchArg) constraint;
    		return new MatchArgInternalNode(allBeanProperties,arg);
        } catch (Exception e) {
        	TranImpl.rollbackAndThrow(e);
        	throw new AssertionError("rollbackAndThrow returned (can't happen)");
        }
	}

	protected MatchOp  op = null;  // We'll always have an op

    public MatchArgTree(MatchOp op) {
    	this.op = op;
    }

    public MatchOp getOp() { return op; }

    public abstract boolean containsNonPrimaryKeyProps();
    public abstract boolean containsMaxOrMin();

    public abstract Property[] getProperties();
    public abstract Object[]   getValues();

    public abstract Iterator<MatchArgLeafNode> leafIterator();
}
