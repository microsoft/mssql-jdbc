//---------------------------------------------------------------------------------------------------------------------------------
// File: ActivityCorrelator.java
//
//
// Microsoft JDBC Driver for SQL Server
// Copyright(c) Microsoft Corporation
// All rights reserved.
// MIT License
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), 
//  to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, 
//  and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS 
//  IN THE SOFTWARE.
//---------------------------------------------------------------------------------------------------------------------------------
 

package com.microsoft.sqlserver.jdbc;

import java.lang.ThreadLocal;
import java.util.UUID;

/**
 * ActivityCorrelator provides the APIs to access the ActivityId in TLS
 */
final class ActivityCorrelator
{

	private static ThreadLocal<ActivityId> ActivityIdTls = new ThreadLocal<ActivityId>() 
	{
	    protected ActivityId initialValue() 
	    {
	        return new ActivityId();
	    }
	};
      
	// Get the current ActivityId in TLS
	static ActivityId getCurrent() 
	{ 
	    // get the value in TLS, not reference
		return ActivityIdTls.get();	
	}
	
	// Increment the Sequence number of the ActivityId in TLS
	// and return the ActivityId with new Sequence number
	static ActivityId getNext() 
	{
		// We need to call get() method on ThreadLocal to get 
		// the current value of ActivityId stored in TLS, 
		// then increment the sequence number.

	    // Get the current ActivityId in TLS
		ActivityId activityId = getCurrent();
        
		// Increment the Sequence number
		activityId.Increment();
	    
		
	    return activityId;	
	}
	
	static void setCurrentActivityIdSentFlag()
	{
		ActivityId activityId = getCurrent();
		activityId.setSentFlag();
	}
		
}
		
class ActivityId {
	private final UUID Id;
	private long Sequence;
	private boolean isSentToServer;
 
	ActivityId() 
	{
	    Id = UUID.randomUUID();
	    Sequence = 0;
	    isSentToServer= false;
	}
  
	UUID getId()
	{
	    return Id;
	}

	long getSequence()
	{
	    return Sequence;
	}
	  
	void Increment()
	{ 
		if (Sequence < 0xffffffffl) //to get to 32-bit unsigned
	    {
	        ++Sequence;
	    }
	    else 
	    {
	        Sequence = 0;
	    }
		
		isSentToServer = false;
	}

	void setSentFlag()
	{
		isSentToServer = true;
	}
	
	boolean IsSentToServer()
	{
		return isSentToServer;
	}
	
	@Override public String toString () 
	{
	    StringBuilder sb = new StringBuilder();
	    sb.append(Id.toString());
	    sb.append("-");
	    sb.append(Sequence);
	    return sb.toString();
	 }
}

