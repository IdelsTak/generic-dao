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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.github.idelstak.genericdao.MatchArg;
import com.github.idelstak.genericdao.RollbackException;
import com.github.idelstak.genericdao.impl.MatchArgTree;
import com.github.idelstak.genericdao.impl.Property;


public class MatchArgInternalNode extends MatchArgTree  {
	private List<MatchArgTree> subNodes = new ArrayList<MatchArgTree>();

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
    public MatchArgInternalNode(Property[] allBeanProperties, LogicMatchArg arg) throws RollbackException {
    	super(arg.getOp());
    	for (MatchArg subConstraint : arg.getArgs()) {
    		subNodes.add(MatchArgTree.buildTree(allBeanProperties,subConstraint));
    	}
    }

    public boolean containsMaxOrMin() {
    	for (MatchArgTree subNode : subNodes) {
    		if (subNode.containsMaxOrMin()) return true;
    	}
    	return false;
    }


    public boolean containsNonPrimaryKeyProps() {
    	for (MatchArgTree subNode : subNodes) {
    		if (subNode.containsNonPrimaryKeyProps()) return true;
    	}
    	return false;
    }

    public List<MatchArgTree> getSubNodes() { return subNodes; }

    public Iterator<MatchArgLeafNode> leafIterator() {
    	return new MyLeafIterator(subNodes);
    }

    public Property[] getProperties() {
    	List<Property> list = new ArrayList<Property>();
    	for (MatchArgTree subNode : subNodes) {
    		list.addAll(Arrays.asList(subNode.getProperties()));
    	}
    	return list.toArray(new Property[list.size()]);
    }

    public Object[] getValues() {
    	List<Object> list = new ArrayList<Object>();
    	for (MatchArgTree subNode : subNodes) {
    		list.addAll(Arrays.asList(subNode.getValues()));
    	}
    	return list.toArray(new Object[list.size()]);
    }

    private static class MyLeafIterator implements Iterator<MatchArgLeafNode> {
    	private List<MatchArgTree> subNodes;
    	private Iterator<MatchArgLeafNode> subIter = null;
    	private int pos = 0;

    	public MyLeafIterator(List<MatchArgTree> subNodes) {
    		this.subNodes = subNodes;
    		if (subNodes.size() > 0) subIter = subNodes.get(0).leafIterator();
    	}

    	public boolean hasNext() {
    		while (pos < subNodes.size()) {
    			if (subIter.hasNext()) return true;
    			pos++;
        		if (pos < subNodes.size()) subIter = subNodes.get(pos).leafIterator();
    		}
    		return false;
    	}

    	public MatchArgLeafNode next() {
    		if (!hasNext()) throw new NoSuchElementException();
    		return subIter.next();
    	}

    	public void remove() {
    		throw new UnsupportedOperationException();
    	}
    }
}
