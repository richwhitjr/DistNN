package com.twitter.lsh.stores

import com.twitter.logging.Logger
import com.twitter.lsh.hashing.HashFamily
import com.twitter.lsh.vector._
import com.twitter.storehaus.FutureOps
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.util.Future

/**
 * Base implementations of the HashTableManager
 * If you want to do something very different extend the trait or else use the Store for
 * most distributed systems.
 */

trait HashTableManager[T] {
  def update(oldKeyVecs: Map[T, BaseLshVector], newKeyVecs: Map[T, (BaseLshVector, BaseLshVector)]):Future[Map[(TableIdentifier, Int), Future[Unit]]]
  def query(vecs: Set[BaseLshVector]): Future[Map[T, Option[BaseLshVector]]]
}

abstract class HashTableManagerStore[T](family: HashFamily,
                                        numHashTables: Int,
                                        numHashes: Int)
  extends HashTableManager[T] {
  lazy val log = Logger("HashTableManager")

  val hashTable: MergeableStore[(TableIdentifier, Int), Set[T]]

  val tables = for (i <- 1 to numHashTables)
    yield new HashTable[T](i, numHashes, hashTable, family)

  /**
   * Delete takes a set of keys and their corresponding vectors and deletes them from all hashtables,
   * removing them from the Set[Key Objects] which they mapped to.
   * @param keyVecs - Map[Key -> Vector]. This vector is expected to be normalized.
   */

  def deleteFromTables(keyVecs: Map[(TableIdentifier, Int), Set[T]]) = {
    FutureOps.mapCollect(hashTable.multiGet(keyVecs.keySet))
      .map(results =>
      hashTable.multiPut(
        results.filter(_._2.isDefined)
          .map{case (k,v) =>
          val set = v.get
          (k, Some(v.get -- keyVecs.get(k).get))
        }))
  }

  /**
   * Update takes a set of keys and their corresponding vectors and adds them to all hashtables.
   * This will add the key to the Set[Key Objects] to which it maps. If the set does not exist, it
   * will be created.
   * @param keyVecs - Map[Key -> Vector]. This vector is expected to be normalized.
   */

  def updateTables(keyVecs: Map[(TableIdentifier, Int), Set[T]]):Future[Map[(TableIdentifier, Int), Future[Unit]]] = {
    log.debug("Update: %s", keyVecs)
    FutureOps.mapCollect(hashTable.multiMerge(keyVecs).mapValues(_ => Future(Future())))
  }

  def removeIntersectionItems(original: Map[(TableIdentifier, Int), Set[T]],
                              intersection: Map[(TableIdentifier, Int), Set[T]]) = {
    original.flatMap { case (k, v) =>
        val clean = intersection.get(k).map(x => v &~ x)
        if (clean.isDefined) {
          val c = clean.get
          if (c.nonEmpty)
            Some(k, c)
          else
            None
        } else Some(k, v)
    }
  }

  def cleanKeys(oldKeys: Map[(TableIdentifier, Int), Set[T]],
                newKeys: Map[(TableIdentifier, Int), Set[T]]):
  (Map[(TableIdentifier, Int), Set[T]], Map[(TableIdentifier, Int), Set[T]]) = {
    val removeItems = (oldKeys.toList ++ newKeys.toList)
      .groupBy(_._1)
      .filter{case (k,v) => v.size > 1}
      .mapValues(list => list(0)._2 & list(1)._2)
      .filter{case (k, set) => set.nonEmpty}

    if (removeItems.size > 0) {
      // Now we have Map[(TableIdentifier, Int), subSet[T] of items to remove from old & new
      (removeIntersectionItems(oldKeys, removeItems), removeIntersectionItems(newKeys, removeItems))
    } else (oldKeys, newKeys)
  }

  def update(deleteVecs: Map[T, BaseLshVector], insertVecs: Map[T, (BaseLshVector, BaseLshVector)]):
    Future[Map[(TableIdentifier, Int), Future[Unit]]]= {
    val delKeys = tables.map(table => table.getKeys(deleteVecs)).flatten.toMap
    val insKeys = tables.map(table => table.getKeys(insertVecs.mapValues(_._2))).flatten.toMap
    val (cleanDelKeys, cleanInsKeys) = cleanKeys(delKeys, insKeys)
    deleteFromTables(cleanDelKeys).onSuccess { _ =>
      updateTables(cleanInsKeys)
      Unit
    }
  }

  /**
   * Query takes a set of keys and their corresponding vectors and returns the union of the sets of
   * objects similar to each key.
   * @param vecs - Set[Normalized LshVectors]
   * @return - Set(Objects similar to Key1) U ... U Set(Objects similar to KeyN)
   */
  def query(vecs: Set[BaseLshVector]): Future[Map[T, Option[BaseLshVector]]] = {
    log.debug("query(vecs:%s)", vecs)
    val items = tables.map(table => table.getKeys(vecs)).flatten.toSet
    log.debug("query items: %s", items)
    FutureOps.mapCollect(hashTable.multiGet(items))
      .onFailure { throwable => log.error("hashTable.multiGet(%s) failed: %s", items, throwable)}
      .map{ results =>
        if(results.size == 0) {
          Map.empty
        } else {
          results.map(_._2.getOrElse(Set.empty)).reduce(_ ++ _).map((_, None)).toMap
        }
      }
  }
}
