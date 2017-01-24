# Java Coding Standards

The purpose of the Java Coding Standards is to create a collaboration baseline. It will be helpful for scenarios where many people are creating, modifying, and contributing to the same project. 


## 0. Use Wisely

**Do NOT** blindly obey these guidelines, use them (after understanding) where they make sense.

## 1. General

All **changed code**, must obey these *coding standards*. As we have huge legacy code we will have code which defers Coding Standards. Eclipse code formatter [mssql-jdbc_formatter.xml](mssql-jdbc_formatter.xml) must be used to format the **changed code**, except for [enum types](#EnumSpec).

## 2. Comments

We should give appropriate java comments in code. Please find ideal example of java comments.  We do not expect to have full javadocs for common methods like getters / setters.  

**Correct:**

```java
/**
 * Get property-only names that do not work with connection String
 * 
 * @param name
 *            to normalize
 * @param logger
 * @return the normalized property name
 */
static String getPropertyOnlyName(String name, Logger logger) {
	if (null == name) {
		return name;
	}
	... some complex logic
}
```

**Incorrect:**

```java
 // Get property-only names
  static String getPropertyOnlyName(String name, Logger logger) {
 	if(null == name) {
		return name;
	}
	...
 }
```
 
## 3. Copyrights

All Java files should contain the appropriate copyright notice at the beginning of the file. 

```java
/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
```

## 4. Good Practices

Pay special attention to the most common style errors...

* classes too long
* methods too long
* little or no javadoc comments
* swallow exceptions
* multiple *return* statements
* Overuse of arrays in place of collections
* too much or no whitespace


### 4.1. Avoid multiple *return* statements

Multiple *return* statements are hard and time consuming to debug.

**Correct:**

```java
 public class StringUtils {

 	public static boolean isEmpty(String string) {
 		return string == null || "".equals(string.trim());
 	}

 }

or

public class StringUtils {

 	public static boolean isEmpty(String string) {
 		boolean empty = false;
 		if (string == null) {
 			empty = true;
 		} else if ("".equals(string.trim())) {
 			empty = true;
 		}
 		return empty;
 	}

 }
```

**Incorrect:**

```java
 public class StringUtils {

 	public static boolean isEmpty(String string) {
 		if (string == null) {
 			return true;
 		} else if ("".equals(string.trim())) {
 			return true;
 		}
 		return false;
 	}

 }
```

### 4.2. Boolean comparisons

Mirroring the natural language "if the current state is not active" rather than "if active is not the current state"

**Correct:**

```java
	!active
```

**Incorrect:**

```java
	active == false
```

### 4.3. *for* loops Vs *for-each* loops

When iterating over iterable elements where the current index in the iteration is not important for-each loops are preferred. **Not compatible for JDK 1.4 **

**Correct:**

```java
	for (String name: names) {
		doStuff(name);
	}
```

**Incorrect:**

```java
	for (int i = 0; i < names.length; i++) {
		doStuff(names[i]);
	}	
```

### 4.4. *String* concatenation

Avoid the use of + or += to concatenate strings. Use java standards designed for that purposes such as String.format, StringBuilder, etc.  If you are doing thread safe operation then use StringBuilder instead of StringBuffer.

**Correct:**

```java
	log.debug(String.format("found %s items", amount));
```

**Incorrect:**

```java
	log.debug("found " + amount + " items");	
```

### 4.5. Collections

Use the right collections for the right task.

**Duplicates**

* Allows duplicates: [List](http://docs.oracle.com/javase/8/docs/api/java/util/List.html)
* Does Not Allow Duplicates: [Set](http://docs.oracle.com/javase/8/docs/api/java/util/Set.html), [Map](http://docs.oracle.com/javase/8/docs/api/java/util/Map.html)

**Implementations Iteration Order**

* [HashSet](http://docs.oracle.com/javase/8/docs/api/java/util/HashSet.html) - undefined
* [ConcurrentHashMap](https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/ConcurrentHashMap.html) - undefined
* [HashMap](http://docs.oracle.com/javase/8/docs/api/java/util/HashMap.html) - undefined
* [LinkedHashSet](http://docs.oracle.com/javase/8/docs/api/java/util/LinkedHashSet.html) - insertion order
* [LinkedHashMap](http://docs.oracle.com/javase/8/docs/api/java/util/LinkedHashMap.html) - insertion order of keys
* [ArrayList](http://docs.oracle.com/javase/8/docs/api/java/util/ArrayList.html) - insertion order
* [LinkedList](http://docs.oracle.com/javase/8/docs/api/java/util/LinkedList.html) - insertion order
* [TreeSet](http://docs.oracle.com/javase/8/docs/api/java/util/TreeSet.html) - ascending order (Comparable / Comparator)

### 4.6. Raw types

Avoid using raw types when using classes that support generics.

**Correct:**

```java
List<String> people = Arrays.asList("you", "me");
```

**Incorrect:**

```java
List people = Arrays.asList("you", "me");
```


## 5. Design Patterns

Consider the use of common design patterns.

### 5.1. Enums <a name="EnumSpec"></a>

Constrain arguments by using type safe enumerations.

**Correct:**

```java
public enum Options {
	YES, NO
}
```

**Incorrect:**

```java
String yes = "YES";
String no = "NO";
```
<br />
enum value with multiple constants must be alligned such that the constant of same type from all the values must be alligned in same column.

**Correct:**
```java
public enum SqlType{
	VARCHAR 	("varchar",		JDBCType.VARCHAR),
	NVARCHAR	("nvarchar",	JDBCType.NVARCHAR),
	CHAR		("char",		JDBCType.CHAR);
}
```

**Incorrect**
```java
public enum SqlType {
	VARCHAR("varchar", JDBCType.VARCHAR),
	NVARCHAR("nvarchar", JDBCType.NVARCHAR),
	CHAR("char", JDBCType.CHAR);
}
```

### 5.2. Private Helpers

Consider private helper methods to break down complex flows and long methods into more readable code.


## 6. Active enforcement

Java developers are expected to use tools that enforce some of these good practices and quality code in general.

 - The de-facto Editor is the Eclipse IDE
 - New developed code should pass SonarQube rules
 - Strongly recommend [Sonar Lint](http://www.sonarlint.org/) tool
 - **New developed code /  Only changed code** should be formatted by Eclipse formatter.

