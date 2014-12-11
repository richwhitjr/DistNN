# Locality Sensitive Hashing Library on Distributed Stores

* Still a work in progress, code will change dramatically *

This library implements the LSH algorithm in a way that the hash tables can be stored in distributed systems such as Redis or Memcache.

It also comes with an option to store everything in memory.  Makes heavy use of the Storehaus library to abstract the data stores.

## Building
There is a script (called sbt) in the root that loads the correct sbt version to build:

1. ./sbt update
2. ./sbt test
3. ./sbt assembly