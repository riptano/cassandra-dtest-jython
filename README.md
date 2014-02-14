## Cassandra Jython Example

Just a simple Jython project that connects to Cassandra and runs a few queries utilizing the DataStax Java Driver.

### Setup

Prerequisites :

 * [ant](https://ant.apache.org)
 * [ivy](https://ant.apache.org/ivy/)
 * A running Cassandra (2.0+) cluster on localhost. For testing, the easiest way is with [ccm](https://github.com/pcmanus/ccm)

After installing ivy, you may need to copy ivy.jar to where ant can see it, such as: ~/.ant/lib/ivy.jar

Simply running `ant` in the root of the checkout will download all of the other dependenices (including Jython and the Cassandra driver) and run paging_test.py. 
