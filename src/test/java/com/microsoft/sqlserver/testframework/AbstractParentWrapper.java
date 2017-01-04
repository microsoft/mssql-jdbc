// ---------------------------------------------------------------------------------------------------------------------------------
// File: AbstractParentWrapper.java
//
//
// Microsoft JDBC Driver for SQL Server
// Copyright(c) Microsoft Corporation
// All rights reserved.
// MIT License
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files(the "Software"),
// to deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense,
// and / or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions :
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.
// ---------------------------------------------------------------------------------------------------------------------------------

package com.microsoft.sqlserver.testframework;

/**
 * Stores the parent class. For connection parent is null; for Statement,
 * Connection is parent; for ResultSet, Statement is parent
 */
public abstract class AbstractParentWrapper {
	static final int ENGINE_EDITION_FOR_SQL_AZURE = 5;

	AbstractParentWrapper parent = null;
	Object internal = null;
	String name = null;

	AbstractParentWrapper(AbstractParentWrapper parent, Object internal, String name) {
		this.parent = parent;
		this.internal = internal;
		this.name = name;
	}

	void setInternal(Object internal) {
		this.internal = internal;
	}

	public Object product() {
		return internal;
	}

	public AbstractParentWrapper parent() {
		return parent;
	}
}
