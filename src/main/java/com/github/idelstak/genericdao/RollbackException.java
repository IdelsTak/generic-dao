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
 * The exception thrown when a CRUD method encounters a problem, such as
 * not being able to connect to the database, deadlock, when invalid parameters
 * are passed, etc.
 * <p>
 * If a user initiated transaction was active when a CRUD method throws
 * <tt>RollbackException</tt> the transaction (by convention) is rolled back before (or in the process of) throwing
 * <tt>RollbackException</tt>.
 */
public class RollbackException extends Exception {
	private static final long serialVersionUID = 1L;

	public RollbackException(String message) {
		super(message);
	}

	public RollbackException(Exception e) {
		super(e);
	}

    public RollbackException(String message, Exception e) {
        super(message,e);
    }
}
