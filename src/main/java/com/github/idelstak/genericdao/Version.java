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

/**
 * This class provides version and build information.
 * You can run this class and it will print out the version information.
 * There is also a constant String you can use to see what version you're using.
 */
public class Version {
    /**
     * A constant String containing the version and build info.
     */
    public static final String VERSION_INFO = "GenericDAO-3.0.2 Build of 26-Jan-2016";

    /**
     * A main method that will print out the version information.
     * This program will be executed if you run a GenericDAO JAR file.
     * @param args the program does not look at it's arguments.
     */
    public static void main(String[] args) {
        System.out.println(VERSION_INFO);
        System.out.println("Documentation available at http://jeffeppinger.com/GenericDAO");
    }
}
