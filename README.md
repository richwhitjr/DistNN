# Locality Sensitive Hashing Library on Distributed Stores

This library implements the LSH algorithm in a way that the hash tables can be stored in distributed systems such as Redis or Memcache.

It also comes with an option to store everything in memory.  Makes heavy use of the Storehaus library to abstract the data stores.

To read more about the algorithm see http://en.wikipedia.org/wiki/Locality-sensitive_hashing

## Building
There is a script (called sbt) in the root that loads the correct sbt version to build:

1. ./sbt update
2. ./sbt test
3. ./sbt assembly

## Quick Start
The easiest place to start would be a simple LSH Server that uses in memory hash tables.

Below is an example that constructs the LSH table and queries it for one vector.
  
```scala
  val vectors = Array(
    IndexedVector(1L, DoubleLshVector(Array(1.0, 2.0, 3.0, 4.0))),
    IndexedVector(2L, DoubleLshVector(Array(3.0, 2.0, 1.0, 9.0))),
    IndexedVector(3L, DoubleLshVector(Array(19.0, 22.0, 13.0, 13.0)))
  )
  
  val lshParams = 
    LshParams(
      hashTables = 1,
      hashFunctions = 5,
      radius = 1.0,
      dimensions = 4
    )

  val lsh = LshEuclideanDoubleVector.withVectors(lshParams, vectors)

  val firstVector = vectors(0)
  val (_, lshResults) = Await.result(lsh.query(firstVector.vector))
  val scoredResults = lsh.scoreQuery(firstVector.vector, lshResults, 10)

  scoredResults.foreach(println)
```