package com.twitter.lsh

import com.twitter.algebird.Monoid
import com.twitter.logging.Logger
import com.twitter.lsh.hashing.HashFamily
import com.twitter.lsh.vector._
import com.twitter.lsh.stores._
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.storehaus.{FutureCollector, FutureOps, ReadableStore}
import com.twitter.util.Future


/**
 * Scala implementation of Locality Sensitive Hashing. This implementation is backed by Stores
 * and allows the LSH mappings to be updated, making it read/write, instead of constructed on
 * startup and only query-able.
 *
 * See http://en.wikipedia.org/wiki/Locality-sensitive_hashing for a description.
 * Briefly, given an item (with a vector), hashes it into N hashtables. Each hashtable computes a
 * different hashcode for it, "wanting" similar items to collide, storing hashcode -> set(items).
 * On query, given an item('s vector), we union all the set(items) it maps to.
 */

/**
 * Encapsulates parameters necessary to construct an LSH instance.
 * @param hashTables - # of hash tables used to back LSH (used by hashTableManager)
 * @param hashFunctions - # of hash functions used by each hash table (used by hashTableManager)
 * @param dimensions - # of dimensions in each vector (used by HashFamily)
 */
case class LshParams(hashTables: Int, hashFunctions: Int, dimensions: Int)

case class ScoredResult[T](key: T, score: Double)

/**
 * See above. Note that two different types of Vectors are used:
 * Raw Vector - This is the vector which represents Item -> Vector
 *   In practice, Raw Vector could be a non-normalized DoubleLshVector or a (non-normalized)
 *   DecayedLshVector or some other type of vector.
 * Hash Vector - This is the normalized vector which is used for hashing.
 *   In practice, Hash Vector is probably DoubleLshVector and will be a normalized version
 *   of Raw Vector.
 *
 * @param family - The type of hashing to use. Examples are Euclidean & Manhattan-distance based.
 * @param vectorStore - StoreHaus Store for storing the vectors.
 * @param hashTableManager - Manager for storing the hashes. Usually also a Storehaus Store
 * @param normFunction - When passed all vectors will be normalized by this function
 * @tparam T - Item type to be stored
 * @tparam U - Vector type of Raw Vector
 */
class Lsh[T, U <: BaseLshVector[U]](family: HashFamily,
                                    vectorStore: MergeableStore[T, U],
                                    hashTableManager: HashTableManager[T, U],
                                    normFunction: U => U)
                                    (implicit val uMonoid: Monoid[U]) {
  val log = Logger("Lsh")

  def addVector(keys: Set[T], value: U) = {
    FutureOps.mapCollect(vectorStore.multiGet(keys))
      .onSuccess { vecs =>
        val newVecs = vecs.mapValues(v => Some(if(v.isDefined) uMonoid.plus(v.get, value) else value))
        FutureOps.mapCollect(vectorStore.multiPut(newVecs))
          .onSuccess { _ =>
            val normOldVecs = vecs.filter(_._2.isDefined).mapValues(v => normFunction(v.get))
            val normNewVecs = newVecs.mapValues(nv => (nv.get, normFunction(nv.get)))
            hashTableManager.update(normOldVecs, normNewVecs)
          }
        }
  }

  protected def queryLshRawResults(vector: U): Future[Map[T, Option[U]]] = {
    hashTableManager.query(Set(vector)).flatMap{ candidates =>
      val (foundVecs, missingVecs) = candidates.partition { case (k, v) => v.isDefined}
      if (missingVecs.nonEmpty) {
        val splitMissing = missingVecs.keySet.grouped(5000).toSeq
          .map(set => FutureOps.mapCollect(vectorStore.multiGet(set))(FutureCollector.bestEffort))

        Future.collect(splitMissing).map(_.reduce(_ ++ _) ++ foundVecs)
      } else Future(foundVecs)
    }
  }

  /**
   * Given a normalized vector (V), returns matching keys (T) and their normalized vectors (V).
    *
    * @param vector - Normalized vector of type V (usually DoubleLshVector)
   * @return (inputVector, Map[keyObject -> normalizedVector])
   */
  protected def queryLshCandidatesStore(vector: U): Future[(U, Map[T, U])] =
    queryLshRawResults(vector).map { candidateVectorMap =>
      (vector, candidateVectorMap.collect { case (k, Some(u)) => (k, normFunction(u)) })
    }

  // This helper method is generally not for public use. It is used internally and by tests.
  protected def scoreQuery(vector: U, candidateMap: Map[T, U], maxResults:Int) =
    candidateMap.mapValues{vec => family.score(vector, vec)}
      .toList
      .sortBy(_._2)
      .take(maxResults)
      .map{case(key, score) => ScoredResult(key, score)}

  /**
    * Given a key (T), returns matching keys (T) and their normalized vectors.
    * @param key - Key object of type T
    * @return ((inputKey, normalizedVector (if found)), Map[keyObject -> normalizedVector])
    */
  protected def queryKeyRaw(key: T):Future[((T, Option[U]), Map[T, U])] =
    vectorStore.get(key).flatMap { optKeyVec => optKeyVec.map { keyVec =>
      queryLshCandidates(keyVec).map { case (vec, map) => ((key, Some(normFunction(vec))), map) }
    }.getOrElse(Future((key, None), Map[T, U]()))}

  /**
    * Given a raw vector (U), returns matching keys (T) and their normalized vectors.
    *
    * @param vector - Raw vector of type U
    * @return (inputVector, Map[keyObject -> normalizedVector])
    */
  def queryLshCandidates(vector: U): Future[(U, Map[T, U])] =
    queryLshCandidatesStore(normFunction(vector)).map(x => (vector, x._2))

  /**
   * Given a key (T), returns the top N matches & scores, based on hash family scoring
    *
    * @param key - Key object of type T
   * @param maxResults - max # of matches to return
   * @return - Matching keys and their scores in descending score order.
   */
  def queryKey(key: T, maxResults: Int): Future[List[ScoredResult[T]]] =
    queryKeyRaw(key).map{case ((_, keyVec), candidateMap) =>
      keyVec.map(kv => scoreQuery(kv, candidateMap, maxResults)).getOrElse(Nil)
    }

  /**
   * Given a vector (V), returns top N matches & scores, based on hash family scoring
    *
   * @param vector - Normalized vector of type V
   * @param maxResults - max # of matches to return
   * @return - Matching keys and their scores in descending score order.
   */
  def queryVector(vector: U, maxResults: Int): Future[List[ScoredResult[T]]] =
    queryLshCandidates(vector).map{case (keyVec, candidateMap) => scoreQuery(keyVec, candidateMap, maxResults)}
}
