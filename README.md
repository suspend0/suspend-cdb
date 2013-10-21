A Constant Database (cdb) in Java
============

An implementation of [the cdb format][cdb] in Java.  Key features
are 

  - Thread safety: readers are independent
  - No-copy: the file is mapped into memory and pointers returned
  - Compatibility: API is java.util.Map

Usage
-------

```java
ByteBuffer key = ...;
ByteBuffer value = ...;
Cdb.Builder builder = Cdb.builder(file);
builder.add(key,value);
builder.build();

Map<ByteBuffer,ByteBuffer> cdb = Cdb.open(file);
cdb.containsKey(key);
```

Dependencies
-------
JUnit, otherwise none.

Status
-------
Some effort has been made to keep this compatible with the cdb 
spec but this is untested.  Also, file lengths are limited to 
Integer.MAX_VALUE rather than 4GB.

[cdb]: http://cr.yp.to/cdb.html
