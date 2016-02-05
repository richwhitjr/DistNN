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

trait HashTableManager[T, U <: BaseLshVector[U]] {
  def update(oldKeyVecs: Map[T, U], newKeyVecs: Map[T, (U, U)]):  Future[Map[(TableIdentifier, Int), Future[Unit]]]
  def query(vecs: Set[U]): Future[Map[T, Option[U]]]
}

abstract class HashTableManagerStore[T, U <: BaseLshVector[U]](family: HashFamily,
                                                               numHashTables: Int,
                                                               numHashes: Int)
  extends HashTableManager[T, U] {
  lazy val log = Logger("HashTableManager")

  val hashTable: MergeableStore[(TableIdentifier, Int), Set[T]]

  val tables = for (i <- 1 to numHashTables)
    yield new HashTable[T, U](i, numHashes, hashTable, family)

  /**
   * Delete takes a set of keys and their corresponding vectors and deletes them from all hashtables,
   * removing them from the Set[Key Objects] which they mapped to.
    *
    * @param keyVecs - Map[Key -> Vector]. This vector is expected to be normalized.
   */

  def deleteFromTables(keyVecs: Map[(TableIdentifier, Int), Set[T]]) = {
    FutureOps.mapCollect(hashTable.multiGet(keyVecs.keySet))
      .map { results =>
        hashTable.multiPut(
          results.collect { case (k, Some(v)) =>
            (k, Some(v -- keyVecs.get(k).get))
          })
      }
  }

    /**
   * Update takes a set of keys and their corresponding vectors and adds them to all hashtables.
   * This will add the key to the Set[Key Objects] to which it maps. If the set does not exist, it
   * will be created.
      *
      * @param keyVecs - Map[Key -> Vector]. This vector is expected to be normalized.
   */

  def updateTables(keyVecs: Map[(TableIdentifier, Int), Set[T]]):Future[Map[(TableIdentifier, Int), Future[Unit]]] = {
    FutureOps.mapCollect(hashTable.multiMerge(keyVecs).mapValues(_ => Future(Future())))
  }

  def removeIntersectionItems(original: Map[(TableIdentifier, Int), Set[T]],
                              intersection: Map[(TableIdentifier, Int), Set[T]]) = {
    original.flatMap { case (k, v) =>
        val clean = intersection.get(k).map(x => v &~ x)
        if (clean.isDefined) {
          val c = clean.get
          if (c.nonEmpty) Some(k, c) else None
        } else Some(k, v)
    }
  }

  def cleanKeys(oldKeys: Map[(TableIdentifier, Int), Set[T]],
                newKeys: Map[(TableIdentifier, Int), Set[T]]):
  (Map[(TableIdentifier, Int), Set[T]], Map[(TableIdentifier, Int), Set[T]]) = {

    val removeItems = (oldKeys.toList ++ newKeys.toList)
      .groupBy(_._1)
      .filter{case(_, v) => v.size > 1}
      .mapValues(list => list.head._2 & list(1)._2)
      .filter{case(_, set) => set.nonEmpty}

    if (removeItems.nonEmpty) {
      (removeIntersectionItems(oldKeys, removeItems), removeIntersectionItems(newKeys, removeItems))
    } else (oldKeys, newKeys)
  }

  def update(deleteVecs: Map[T, U], insertVecs: Map[T, (U, U)]):
    Future[Map[(TableIdentifier, Int), Future[Unit]]]= {

    val delKeys = tables.flatMap(_.getKeys(deleteVecs)).toMap
    val insKeys = tables.flatMap(_.getKeys(insertVecs.mapValues(_._2))).toMap
    val (cleanDelKeys, cleanInsKeys) = cleanKeys(delKeys, insKeys)

    deleteFromTables(cleanDelKeys).onSuccess { _ => updateTables(cleanInsKeys)}
  }

  /**
   * Query takes a set of keys and their corresponding vectors and returns the union of the sets of
   * objects similar to each key.
    *
    * @param vecs - Set[Normalized LshVectors]
   * @return - Set(Objects similar to Key1) U ... U Set(Objects similar to KeyN)
   */
  def query(vecs: Set[U]): Future[Map[T, Option[U]]] = {
    val items = tables.flatMap(_.getKeys(vecs)).toSet
    FutureOps.mapCollect(hashTable.multiGet(items))
      .onFailure { throwable => log.error("hashTable.multiGet(%s) failed: %s", items, throwable)}
      .map{ results =>
        results
          .map(_._2.getOrElse(Set.empty))
          .reduce(_ ++ _)
          .map((_, None))
          .toMap
      }
  }
}
