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

import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;


public class MyPrintWriter extends PrintWriter {
	public MyPrintWriter(Writer w) {
		super(w);
	}

	public MyPrintWriter(OutputStream out) {
		super(out);
	}

	public void println(String s) {
		super.println(Thread.currentThread().getName() + ": " + s);
		flush();
	}
}
