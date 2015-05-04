package com.twitter.lsh.stores

import com.twitter.logging.Logger
import com.twitter.lsh.hashing.HashFamily
import com.twitter.lsh.vector._
import com.twitter.storehaus.FutureOps
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.util.{Future, SynchronizedLruMap}

class BaseHashTable[T](id: Int, numHashes: Int, family: HashFamily) {
  val hashFunctions = for (i <- 1 to numHashes) yield family.createHasher(id, i)
  val tableId = new TableIdentifier(id, numHashes, family.hashCode)

  def hash(vector: BaseLshVector): Int =
    family.combine(hashFunctions.map(_.hash(vector.toDoubleVec)).toArray)

  def getKeys(vecs: Set[BaseLshVector]): Set[(TableIdentifier, Int)] = {
    vecs.map(vec => (tableId, hash(vec)))
  }
}

/**
 * CachingHashTableStore caches the calculated vector hashes for efficiency. It does this by
 * replacing the hash method in BaseHashTable to store in a local LRU Map.
 *
 * @param id - See BaseHashTable
 * @param numHashes - See BaseHashTable
 * @param family - See BaseHashTable
 * @param size - Cache Size. Defaults to 98,300 entries (just under 2^17*0.75 to avoid resizing).
 * @tparam T - See BaseHashTable
 */
class CachingHashTable[T](id: Int, numHashes: Int, family: HashFamily, size: Int = 98300) // ~2^17*0.75
  extends BaseHashTable[T](id, numHashes, family) {
  val hashCache = new SynchronizedLruMap[BaseLshVector, Int](size)

  /**
   * Given a vector, compute its hash. Uses an internal HashMap for efficiency.
   * @param vector - LshVector to hash. This vector is expected to be normalized.
   * @return - Int hashcode.
   */
  override def hash(vector: BaseLshVector): Int = {
    hashCache.getOrElse(vector, {
      // If not in cache, compute, store, and return
      val hashVal = family.combine(hashFunctions.map(_.hash(vector.toDoubleVec)).toArray)
      hashCache.put(vector, hashVal)
      hashVal
    })
  }
}

trait StoringHashTable[T] {
  def add(keyVecs: Map[T, BaseLshVector])
  def delete(keyVecs: Map[T, BaseLshVector])
  def query(vector: BaseLshVector): Future[Option[Set[T]]]
}

/**
 * HashTable represents an individual hashtable backed by storehaus.
 * Each hashtable consists of multiple hash functions which are constructed from the hash family,
 * a cache of previously hashed Vectors, and a unique, deterministic table identifier.
 *
 * While HashTableStores have the ability to interact with memcache directly, it is preferable to
 * use HashTableManager to reduce the number of separate requests to memcache.
 * @param id - Hashtable Id, should usually just be between 1..# of hashtables being created.
 * @param numHashes - # of hashes functions to use when hashing items.
 * @param store - The memcache for read/write.
 * @param family - The HashFamily to use for hash creation.
 * @tparam T - Key type, the item users ultimately want.
 */
class HashTable[T](id: Int, numHashes: Int,
                   store: MergeableStore[(TableIdentifier, Int), Set[T]],
                   family: HashFamily) extends CachingHashTable[T](id, numHashes, family)
  with StoringHashTable[T] {
  lazy val log = Logger(s"HashTable[$id]")
  /**
   * Given a set of Keys and their vectors, returns (tableIdentifier, hash of vector) -> Key
   * (Actually the returned value is Set(Key) so that it can be used for .merge and Set difference)
   * The returned maps allow .multi(Get|Put|Merge) operations on memcache.
   * @param keyVecs - Map(Key -> Normalized Vector)
   * @tparam T - Key type
   * @return - Map((Table Identifier, Hashcode) -> Set(Key))
   */
  def getKeys[T](keyVecs: Map[T, BaseLshVector]): Map[(TableIdentifier, Int), Set[T]] = {
    keyVecs.mapValues(vec => (tableId, hash(vec))).map(_.swap).mapValues(Set(_))
  }

  def add(keyVecs: Map[T, BaseLshVector]) = {
    store.multiMerge(keyVecs.map(_.swap).map{case(k,v) => ((tableId, hash(k)), Set(v))})
  }

  def delete(keyVecs: Map[T, BaseLshVector]) = {
    val doubleKeyToStringMap = keyVecs.mapValues(vec => (tableId, hash(vec))).map(_.swap)
    FutureOps.mapCollect(store.multiGet(doubleKeyToStringMap.keySet))
      .map(results => store.multiPut(results.filter(_._2.isDefined).map{case (k,v) =>
      (k, Some(v.get -- Set(doubleKeyToStringMap.get(k).get)))}))
  }

  def query(vector: BaseLshVector): Future[Option[Set[T]]] = {
    store.get((tableId, hash(vector)))
  }
}
