## Cassandra Jython Dtests

Just a simple Jython project that connects to Cassandra and runs tests utilizing the DataStax Java Driver.

### Setup

Prerequisites :

 * [ant](https://ant.apache.org)
 * [ivy](https://ant.apache.org/ivy/)

After installing ivy, you may need to copy ivy.jar to where ant can see it, such as: ~/.ant/lib/ivy.jar

Simply running `ant` in the root of the checkout will download all of the other dependenices (including Jython and the Cassandra driver) and run paging_test.py. 

Run `ant run_tests` to run the tests.
