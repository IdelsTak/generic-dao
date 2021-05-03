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
 * The exception thrown by a CRUD method (typically from the <tt>create()</tt> method)
 * to signal an attempt to create a new bean in a table that has the same primary key as an
 * existing bean.  (The attempt has failed.)
 * <p>
 * The <tt>DuplicateKeyException</tt> this a subclass of <tt>RollbackException</tt>.
 * As with <tt>RollbackException</tt>, the work done by the transaction that was active when this
 * exception is thrown is rolled back.
 */
public class DuplicateKeyException extends RollbackException {
	private static final long serialVersionUID = 1L;

	public DuplicateKeyException(String message) {
		super(message);
	}
}
