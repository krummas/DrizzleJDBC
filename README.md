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
* `serverCertificate=/path/to/certificate` - when using ssl to connect, allow to provide a reference to a certificate file for validating the server
* `allowMultiQueries=true` - allow sending several queries in one round trip to the server
* `connectTimeout=X` - have an X second connection timeout.
* `createDB=true` - create the given database if it does not exist when connecting.
* `enableBlobStreaming=true` - experimental support for PBMS blob streaming.
* `noPrepStmtCache=true` - Disable prepared statement cache.
* `stripQueryComments=false` - Disable prepared statement comments stripping.
* `serverPublicKey=/path/to/publickey` - If sha256 password is used, the public can either be provided in this file or will be fetched directly from the server.
* `enabledProtocols=protocol1,procotol2` - Force SSL protocol version list with a comma separated list with no space. Default is TLSv1,TLSv1.1,TLSv1.2
* `enabledCipherSuites=cipher1,cipher2` - Enable only the given list of ciphers suites for this connection. Default is to allow all cipher suites supported by the running JVM

As of v1.5.10, additional options allow for passing through proxy-protocol specific information:
* `proxyProtocol=false` - enable or disable proxy protocol functionality. Default is false, which disables the feature. 
When enabled, the information bellow is passed through as a PROXY header upon connection establishment to the MySQL 
server (which must have proxy protocol enabled) according to proxy-protocol v1 spec found here: 
http://www.haproxy.org/download/1.8/doc/proxy-protocol.txt
Please refer to section 2.1 of this specification for details about individual the individual entries found below:
* `proxyProtocol.tcpVersion`
* `proxyProtocol.clientAddress`
* `proxyProtocol.connectedToIPAddress`
* `proxyProtocol.clientPort`
* `proxyProtocol.localPort`


Building and testing
--------------------
To test you need to have:
* a drizzle server running on localhost:3307
* a mysql server running on localhost:3306

each server also needs to have a database callet `test_units_jdbc` and for the mysql one, full read/write rights to an anonymous user.

To build it without running tests, simply do `mvn -Dmaven.test.skip=true package`