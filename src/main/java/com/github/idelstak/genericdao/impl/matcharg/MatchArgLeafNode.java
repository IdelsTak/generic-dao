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

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.github.idelstak.genericdao.impl.MatchArgTree;
import com.github.idelstak.genericdao.impl.Property;

public class MatchArgLeafNode extends MatchArgTree {
    private Property property;
    private Object matchValue = null;

    /*
     * Note this method validates properties and types of the values, but does
     * not clone them Problems found cause IllegalArgumentException or
     * NullPointerException to be thrown. All exceptions (including
     * IllegalArgumentException and NullPointerException) are caught and chained
     * in RollbackException to ensure any active transaction for this thread is
     * rolled back.
     *
     * Checks for the following: No array properties (but byte[] is not checked
     * as it's allowed for EQUALS)
     *
     * For binary operators: no null match values for primary key properties
     * match value must be of same type as property * no null values for
     * non-nullable fields
     */
    public MatchArgLeafNode(Property[] allBeanProperties, UnaryMatchArg arg) {
        super(arg.getOp());

        // propertyForName throws IllegalArgumentException if the property name is not valid
        property = Property.propertyForName(allBeanProperties, arg.getKeyName());

        // Valid for matching the max/min values of numbers, Dates or Strings

        if (!isNumber() && !isDate() && !isString()) {
            throw new IllegalArgumentException(op + " cannot be applied to this property type: " + property);
        }

        // Note: no matchingTypeCheck as no value for unary ops
    }

    public MatchArgLeafNode(Property[] allBeanProperties, BinaryMatchArg arg) {
        super(arg.getOp());

        // propertyForName throws IllegalArgumentException if the property name
        // is not valid
        property = Property.propertyForName(allBeanProperties, arg.getFieldName());

        matchValue = arg.getFieldValue();

        matchingTypeCheck();

        switch (op) {
        case EQUALS:
        case NOT_EQUALS:
            // Valid for comparing any types, except arrays (byte[] is okay)
            break;
        case GREATER:
        case GREATER_OR_EQUALS:
        case LESS:
        case LESS_OR_EQUALS:
            // Valid for comparing numbers, Dates, or Strings
            if (!isNumber() && !isDate() && !isString()) {
                throw new IllegalArgumentException(op + " cannot be applied to this property type: " + property);
            }
            break;
        case CONTAINS:
        case STARTS_WITH:
        case ENDS_WITH:
        case EQUALS_IGNORE_CASE:
        case CONTAINS_IGNORE_CASE:
        case STARTS_WITH_IGNORE_CASE:
        case ENDS_WITH_IGNORE_CASE:
            // Valid for matching String properties, only
            if (!isString()) {
                throw new IllegalArgumentException(op + " cannot be applied to this property type: " + property);
            }
            break;
        default:
            throw new AssertionError("Unknown op: " + op);
        }
    }

    public void fixConstraint(MatchOp newOp, Object newValue) {
        op = newOp;
        matchValue = newValue;
    }

    public Property getProperty() {
        return property;
    }

    public Property[] getProperties() {
        return new Property[] { property };
    }

    public Object getValue() {
        return matchValue;
    }

    public Object[] getValues() {
        return new Object[] { matchValue };
    }

    public Iterator<MatchArgLeafNode> leafIterator() {
        return new MyLeafIterator(this);
    }

    public boolean containsMaxOrMin() {
        if (op == MatchOp.MAX)
            return true;
        if (op == MatchOp.MIN)
            return true;
        return false;
    }

    public boolean containsNonPrimaryKeyProps() {
        return !property.isPrimaryKeyProperty();
    }

    private boolean isDate() {
        Class<?> c = property.getType();
        if (c == java.util.Date.class)
            return true;
        if (c == java.sql.Date.class)
            return true;
        if (c == java.sql.Time.class)
            return true;
        return false;
    }

    private boolean isNumber() {
        Class<?> c = property.getType();
        if (c == float.class)
            return true;
        if (c == int.class)
            return true;
        if (c == double.class)
            return true;
        if (c == long.class)
            return true;
        return false;
    }

    private boolean isString() {
        return property.getType() == String.class;
    }

    private void matchingTypeCheck() {
        if (property.isPrimaryKeyProperty() && matchValue == null)
            throw new IllegalArgumentException("Primary key constraint value cannot be null: property="
                    + property.getName());
        if (matchValue != null && !property.isInstance(matchValue))
            throw new IllegalArgumentException("Constraint value for property " + property.getName()
                    + " is not instance of " + property.getType() + ".  Rather it is " + matchValue.getClass());
        if (matchValue == null && !property.isNullable())
            throw new IllegalArgumentException("Constraint value for property " + property.getName()
                    + " cannot be null");
    }

    private static class MyLeafIterator implements Iterator<MatchArgLeafNode> {
        private MatchArgLeafNode node;

        public MyLeafIterator(MatchArgLeafNode node) {
            this.node = node;
        }

        public boolean hasNext() {
            return node != null;
        }

        public MatchArgLeafNode next() {
            if (node == null)
                throw new NoSuchElementException();
            MatchArgLeafNode answer = node;
            node = null;
            return answer;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
