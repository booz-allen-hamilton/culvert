# Culvert: A Robust Framework for Secondary Indexing #

Secondary indexing is a common design pattern in BigTable-like databases that allows users to index one or more columns in a table. This technique enables fast search of records in a database based on a particular column instead of the row id, thus enabling relational-style semantics in a NoSQL environment. This is accomplished by representing the index either in a reserved namespace in the table or another index table. Despite the fact that this is a common design pattern in BigTable-based applications, most implementations of this practice to date have been tightly coupled with a particular application. As a result, few general-purpose frameworks for secondary indexing on BigTable-like databases exist, and those that do are tied to a particular implementation of the BigTable model.

We developed a solution to this problem called Culvert that supports online index updates as well as a variation of the HIVE query language. In designing Culvert, we sought to make the solution pluggable so that it can be used on any of the many BigTable-like databases (HBase, Cassandra, etc.). Our goal with Culvert is to make an easy, extensible tool for use in the entire NoSQL community.

##Building##

Requirements:
1. Java 1.5
2. Maven 3 (though Maven 2 may work).
3. Hbase 0.91

To install:
1. Pull down the source and run: 
   mvn clean package
This outputs a compiled jar.
2. Install the jar on the classpath of all the servers hosting your table. 
3. Install the jar on the local server from which to issue requests.
4. Create an index table and update your configurations
5. Create an instance of a com.bah.culvert.Client
6. Write your data into your primary table through the Client.

## Community ##

Culvert is a brand new project and we are continually looking to grow the community. We welcome any input, thoughts, patches, etc. 

You can find help or talk development on IRC at
culvert on irc.freenode.net

## Disclaimer ##
Currently Culvert is based on the HBase-0.91 snapshot (the current trunk) which is particularly volatile. As such, some slight code modification is likely to be necessary to work 'out of the box'. We are currently working to create our own release of hbase or moving to a stable version. 

Culvert is provided AS-IS, under the Apache License. See LICENSE.txt for full description.
