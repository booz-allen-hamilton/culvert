# Culvert: A Robust Framework for Secondary Indexing #

Secondary indexing is a common design pattern in BigTable-like databases that allows users to index one or more columns in a table. This technique enables fast search of records in a database based on a particular column instead of the row id, thus enabling relational-style semantics in a NoSQL environment. This is accomplished by representing the index either in a reserved namespace in the table or another index table. Despite the fact that this is a common design pattern in BigTable-based applications, most implementations of this practice to date have been tightly coupled with a particular application. As a result, few general-purpose frameworks for secondary indexing on BigTable-like databases exist, and those that do are tied to a particular implementation of the BigTable model.

We developed a solution to this problem called Culvert that supports online index updates as well as a variation of the HIVE query language. In designing Culvert, we sought to make the solution pluggable so that it can be used on any of the many BigTable-like databases (HBase, Cassandra, etc.). Our goal with Culvert is to make an easy, extensible tool for use in the entire NoSQL community.

## Building ##

Requirements:

1. Java 1.5
2. Maven 3 (though Maven 2 may work).
3. Hbase 0.91

To install:

1.  Pull down the source and run: "mvn clean package". This outputs a compiled jar.
2. Install the jar on the classpath of all the servers hosting your table. 
3. Install the jar on the local server (the 'client') from which to issue requests.
4. Create an index table and update your configurations
5. Create an instance of a com.bah.culvert.Client
6. Write your data into your primary table through the Client.

## Resources ##

All support resources for Culvert are present under resources/.
Currently, the folder consists of:

* CulvertFormat.xml - Formatting for eclipse of the code. 
	Set this for all the Culvert projects from Preferences > Java > Code Style >Formatter

## Roadmap ##

1. Switch Joins to first attempting to use an in-memory table, server side, before dumping results into a 'scratch' table
2. Enable higher consistency puts through use of coprocessors in HBase 
    
    1. Switch to doing the table put before the index
    2. Actually use CPs to ensure that a put has been made before updating the index (two phase commit)
3. Adding support for removes (consistent or otherwise)
4. Add support for batch indexing existing tables
5. Add more index types
	
	1. Document Partitioned Index
	2. N-grams index
	3. Numeric indexes (integer, float, etc)
	4. Web URL index

## Community ##

Culvert is a brand new project and we are continually looking to grow the community. We welcome any input, thoughts, patches, etc. 

### Help

You can find help or talk development on IRC at
\#culvert on irc.freenode.net

Information on how to use culvert is also available at [this blog post](http://jyates.github.com/2011/11/17/intro-to-culvert.html).

The original slides from the presentation at Hadoop Summit 2011 are available on [slideshare](http://www.slideshare.net/jesse_yates/culvert-a-robust-framework-for-secondary-indexing-of-structured-and-unstructured-data)

## Disclaimer ##
Culvert is provided AS-IS, under the Apache License. See LICENSE.txt for full description.
