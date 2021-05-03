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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import com.github.idelstak.genericdao.DAOException;
import com.github.idelstak.genericdao.MaxSize;
import com.github.idelstak.genericdao.PrimaryKey;

public class Property implements Comparable<Property> {

    private static final Integer INTEGER_ZERO = new Integer(0);
    private static final Long LONG_ZERO = new Long(0);
    private static final Float FLOAT_ZERO = new Float(0);
    private static final Double DOUBLE_ZERO = new Double(0);

    private static Class<?> classForName(String s) {
        if (s.equals("java.lang.String")) {
            return String.class;
        }

        if (s.equals("boolean")) {
            return boolean.class;
        }
        if (s.equals("byte[]")) {
            return byte[].class;
        }
        if (s.equals("int")) {
            return int.class;
        }
        if (s.equals("long")) {
            return long.class;
        }
        if (s.equals("double")) {
            return double.class;
        }
        if (s.equals("float")) {
            return float.class;
        }

        if (s.equals("java.sql.Date")) {
            return java.sql.Date.class;
        }
        if (s.equals("java.util.Date")) {
            return java.util.Date.class;
        }
        if (s.equals("java.sql.Time")) {
            return java.sql.Time.class;
        }

        return null;
    }

    private static void checkForDuplicateProperties(Property[] property) throws DAOException {
        for (int i = 0; i < property.length; i++) {
            if (getPropertyNum(property, property[i].getName()) != i) {
                throw new DAOException("Duplicate property names: "
                        + property[getPropertyNum(property, property[i].getName())] + " and " + property[i]);
            }
        }
    }

    private static <T> List<String> derivePrimaryKeyPropertyNames(Class<T> beanClass) throws DAOException {
        List<String> nameList = new ArrayList<String>();

        PrimaryKey annotation = beanClass.getAnnotation(PrimaryKey.class);
        if (annotation == null) {
            return nameList;
        }

        String value = annotation.value();
        String[] names = value.split(",");
        nameList.addAll(Arrays.asList(names));
        return nameList;
    }

    public static <T> Property[] deriveProperties(Class<T> beanClass, boolean lowerCaseColumnNames) throws DAOException {
        List<String> primaryKeyPropertyNames = derivePrimaryKeyPropertyNames(beanClass);
        ArrayList<Property> list = new ArrayList<>();

        Method[] methods = beanClass.getDeclaredMethods();
        for (Method method : methods) {
            String methodName = method.getName();
            Class<?> propType = method.getReturnType();
            if (methodName.length() > 2 && methodName.startsWith("is") && propType == boolean.class) {
                checkGetterParameterCount(method);
                String propName = methodName.substring(2, 3).toLowerCase() + methodName.substring(3);
                Property p = deriveProperty(propName, propType, beanClass, method, primaryKeyPropertyNames, lowerCaseColumnNames);
                if (p != null) {
                    list.add(p);
                }
            } else if (methodName.length() > 2 && methodName.startsWith("is")) {
                throw new DAOException("Methods starting with \"is\" must have boolean return type: " + methodName);
            } else if (methodName.length() > 3 && methodName.startsWith("get")) {
                checkGetterParameterCount(method);
                String propName = methodName.substring(3, 4).toLowerCase() + methodName.substring(4);
                Property p = deriveProperty(propName, propType, beanClass, method, primaryKeyPropertyNames, lowerCaseColumnNames);
                if (p != null) {
                    list.add(p);
                }
            }
        }

        Property[] properties = new Property[list.size()];
        list.toArray(properties);
        setPrimaryKeyPropertyNumbers(properties, primaryKeyPropertyNames);
        Arrays.sort(properties);
        for (int i = 0; i < properties.length; i++) {
            properties[i].propertyNum = i;
        }
        checkForDuplicateProperties(properties);
        checkForSettersWithoutGetters(beanClass, properties);
        return properties;
    }

    private static <T> void checkForSettersWithoutGetters(Class<T> beanClass, Property[] properties) throws DAOException {
        Method[] methods = beanClass.getMethods();
        for (Method method : methods) {
            String methodName = method.getName();
            if (methodName.length() > 3 && methodName.startsWith("set")) {
                if (method.getParameterCount() != 1) {
                    throw new DAOException("Setter method must take one parameter: " + method.getName());
                }
                String propName = methodName.substring(3, 4).toLowerCase() + methodName.substring(4);
                try {
                    // propertyForName throws IllegalArgumentException if the property name is not valid
                    Property p = propertyForName(properties, propName);
                    if (p.columnType != method.getParameterTypes()[0]) {
                        throw new DAOException("Setter parameter type does not match getter parameter type: "
                                + method.getName());
                    }
                } catch (IllegalArgumentException e) {
                    throw new DAOException("Setter does not have matching getter: " + method.getName());
                }
            }
        }
    }

    private static void checkGetterParameterCount(Method method) throws DAOException {
        if (method.getParameterCount() != 0) {
            throw new DAOException("Getter method must take zero parameters: " + method.getName());
        }
    }

    private static Property deriveProperty(String propName, Class<?> propType, Class<?> beanClass, Method getter,
            List<String> primaryKeyPropertyNames, boolean lowerCaseColumnNames) throws DAOException {
        if (propType == void.class) {
            throw new DAOException("Getter cannot return void: property name = " + propName);
        }

        String setterName = "set" + propName.substring(0, 1).toUpperCase() + propName.substring(1);
        try {
            beanClass.getMethod(setterName, propType);
            return getInstance(propName, propType, beanClass, primaryKeyPropertyNames.contains(propName), lowerCaseColumnNames);
        } catch (NoSuchMethodException e) {
            // throw new DAOException("Cannot find matching setter for: " + getter.getName() + "(" + propType.getName() + ")");
            // Now allowing getters without setters
            return null;
        }
    }

    // Pass primaryKeyProperties only when getting non-primary key properties
    // This can be done because we already know the primary key properties when
    // we're looking for
    // the non-primary key properties.
    private static Property getInstance(String propertyName, Class<?> type, Class<?> beanClass,
            boolean isPrimaryKeyProperty, boolean lowerCaseColumnNames) throws DAOException {
        if (type == byte[].class) {
            return new Property(propertyName, type, isPrimaryKeyProperty, lowerCaseColumnNames, beanClass);
        }

        if (classForName(type.getName()) != null) {
            return new Property(propertyName, type, isPrimaryKeyProperty, lowerCaseColumnNames, beanClass);
        }

        throw new DAOException("Cannot map this class type: " + type.getCanonicalName() + " (property name: "
                + propertyName + ").");
    }

    private static int getPropertyNum(Property[] properties, String name) {
        for (int i = 0; i < properties.length; i++) {
            if (properties[i].getName().equalsIgnoreCase(name)) {
                return i;
            }
        }
        throw new AssertionError("Could not find property: " + name);
    }

    public static Property propertyForName(Property[] properties, String propertyName) {
        for (int i = 0; i < properties.length; i++) {
            if (properties[i].getName().equals(propertyName)) {
                return properties[i];
            }
        }
        throw new IllegalArgumentException("No property with name = " + propertyName);
    }

    private static void setPrimaryKeyPropertyNumbers(Property[] properties, List<String> priKeyPropNames)
            throws DAOException {
        for (int i = 0; i < priKeyPropNames.size(); i++) {
            boolean found = false;
            for (Property prop : properties) {
                if (prop.getName().equals(priKeyPropNames.get(i))) {
                    found = true;
                    prop.propertyNum = i;
                }
            }
            if (!found) {
                throw new DAOException("Could not find getter/setter pair for primary key property: "
                        + priKeyPropNames.get(i));
            }
        }
    }
    private String name;
    private Method getter;
    private Method setter;
    private Class<?> type;
    private boolean primaryKeyProperty;
    private int columnMaxStrLen;
    private String columnName;
    private Class<?> columnType;
    private int propertyNum = -1; // Set by deriveProperties()

    protected Property(String name, Class<?> type, boolean isPrimaryKeyProperty, boolean lowerCaseColumnNames,
            Class<?> beanClass) throws DAOException {
        this.name = name;
        this.type = type;
        this.primaryKeyProperty = isPrimaryKeyProperty;

        String capName = name.substring(0, 1).toUpperCase() + name.substring(1);

        try {
            getter = beanClass.getMethod("get" + capName);
        } catch (NoSuchMethodException e) {
            if (type != boolean.class) {
                throw new DAOException(beanClass.getName() + " doesn't match table: no get" + capName
                        + "() method.  Drop the table and let GenericDAO recreate it.");
            }
            try {
                getter = beanClass.getMethod("is" + capName);
            } catch (NoSuchMethodException e2) {
                throw new DAOException(beanClass.getName() + " doesn't match table: no get" + capName + "() or is"
                        + capName + "() method.  Drop the table and let GenericDAO recreate it.");
            }
        }

        if (getter.getReturnType() != type) {
            throw new DAOException(beanClass.getName() + " doesn't match table: get" + capName + "() returns "
                    + getter.getReturnType().getCanonicalName() + " (not " + type.getCanonicalName()
                    + ", which is the table's type).  Drop the table and let GenericDAO recreate it.");
        }

        try {
            setter = beanClass.getMethod("set" + capName, type);
            if (type == String.class) {
                MaxSize annotation = setter.getAnnotation(MaxSize.class);
                if (annotation == null) {
                    columnMaxStrLen = 255;
                } else {
                    columnMaxStrLen = annotation.value();
                }
            }
        } catch (NoSuchMethodException e) {
            throw new DAOException(beanClass.getName() + " doesn't match table: no set" + capName + "("
                    + type.getCanonicalName() + ") method.  Drop the table and let GenericDAO recreate it.");
        }

        columnName = lowerCaseColumnNames ? name.toLowerCase() : name;
        columnType = type;
    }

    @Override
    public int compareTo(Property other) {
        boolean thisPrimary = (this.primaryKeyProperty);
        boolean otherPrimary = (other.primaryKeyProperty);

        if (thisPrimary && otherPrimary) {
            return getPropertyNum() - other.getPropertyNum();
        }

        if (thisPrimary) {
            return -1;
        }
        if (otherPrimary) {
            return 1;
        }

        int c = name.compareTo(other.name);
        if (c != 0) {
            return c;
        }

        return type.getName().compareTo(other.type.getName());
    }

    public boolean equals(Object obj) {
        if (obj instanceof Property) {
            Property other = (Property) obj;
            return compareTo(other) == 0;
        }
        return false;
    }

    public int getColumnMaxStrLen() {
        return columnMaxStrLen;
    }

    public String getColumnName() {
        return columnName;
    }

    public Class<?> getColumnType() {
        return columnType;
    }

    public Object getDefaultValue() {
        if (type == int.class) {
            return INTEGER_ZERO;
        }
        if (type == long.class) {
            return LONG_ZERO;
        }
        if (type == boolean.class) {
            return Boolean.FALSE;
        }
        if (type == float.class) {
            return FLOAT_ZERO;
        }
        if (type == double.class) {
            return DOUBLE_ZERO;
        }
        return null;
    }

    public Method getGetter() {
        return getter;
    }

    public String getName() {
        return name;
    }

    public int getPropertyNum() {
        if (propertyNum < 0) {
            throw new AssertionError("getColumnNum() called before setColumnNum(): " + this);
        }
        return propertyNum;
    }

    public Method getSetter() {
        return setter;
    }

    public Class<?> getType() {
        return type;
    }

    public int hashCode() {
        return name.hashCode();
    }

    public boolean isInstance(Object value) {
        if (type == boolean.class) {
            return value instanceof Boolean;
        }
        if (type == double.class) {
            return value instanceof Double;
        }
        if (type == float.class) {
            return value instanceof Float;
        }
        if (type == int.class) {
            return value instanceof Integer;
        }
        if (type == long.class) {
            return value instanceof Long;
        }
        return type.isInstance(value);
    }

    public boolean isNullable() {
        if (type == boolean.class) {
            return false;
        }
        if (type == double.class) {
            return false;
        }
        if (type == float.class) {
            return false;
        }
        if (type == int.class) {
            return false;
        }
        if (type == long.class) {
            return false;
        }
        return true;
    }

    public boolean isPrimaryKeyProperty() {
        return primaryKeyProperty;
    }

    public String toString() {
        StringBuffer b = new StringBuffer();
        b.append(this.getClass().getSimpleName());
        b.append('#');
        b.append(propertyNum);
        b.append("(");
        if (primaryKeyProperty) {
            b.append("PriKey,");
        }
        b.append("name=");
        b.append(name);
        b.append(",type=");
        b.append(type.getName());
        b.append(')');
        return b.toString();
    }
}
