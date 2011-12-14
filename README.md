Drizzle JDBC
============
A BSD Licensed JDBC driver for MySQL and Drizzle.

Maven
-----

    <dependency>
        <groupId>org.drizzle.jdbc</groupId>
        <artifactId>drizzle-jdbc</artifactId>
        <version>1.1</version>
    </dependency>


Connection string
-----------------
`jdbc:drizzle://<user>@<host>:<port>/<database>`

or

`jdbc:mysql:thin://<user>@<host>:<port>/<database>`

Building and testing
--------------------
To test you need to have:
* a drizzle server running on localhost:3307
* a mysql server running on localhost:3306

each server also needs to have a database callet `test_units_jdbc` and for the mysql one, full read/write rights to an anonymous user.

To build it without running tests, simply do `mvn -Dmaven.test.skip=true package`