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
 * The exception typically thrown by non-CRUD methods in the GenericDAO package.
 * We didn't make <tt>RollbackException</tt> a subclass of DAOException because we
 * want people to be more careful about the added semantics of RollbackException.
 *
 */
public class DAOException extends Exception {
	private static final long serialVersionUID = 1L;

	public DAOException(String message)  { super(message); }

	public DAOException(Throwable cause) { super(cause);   }

	public DAOException(String message, Throwable cause) {
		super(message,cause);
	}
}
