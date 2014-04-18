Drizzle JDBC
============
A BSD Licensed JDBC driver for MySQL and Drizzle.

Maven
-----

    <dependency>
        <groupId>org.drizzle.jdbc</groupId>
        <artifactId>drizzle-jdbc</artifactId>
        <version>1.2</version>
    </dependency>


Driver class
------------
`org.drizzle.jdbc.DrizzleDriver`

Connection string
-----------------
`jdbc:drizzle://<user>@<host>:<port>/<database>`

or

`jdbc:mysql:thin://<user>@<host>:<port>/<database>`

Connection options
------------------
Connection options are appended to the connection string, like a http url.

Current supported options are;

* `useSSL=true` - use ssl to connect (you need to do some java ssl magic to get it to work, look at the mysql documentation)
* `allowMultiQueries=true` - allow sending several queries in one round trip to the server
* `connectTimeout=X` - have an X second connection timeout.
* `createDB=true` - create the given database if it does not exist when connecting.
* `enableBlobStreaming=true` - experimental support for PBMS blob streaming.


Building and testing
--------------------
To test you need to have:
* a drizzle server running on localhost:3307
* a mysql server running on localhost:3306

each server also needs to have a database callet `test_units_jdbc` and for the mysql one, full read/write rights to an anonymous user.

To build it without running tests, simply do `mvn -Dmaven.test.skip=true package`