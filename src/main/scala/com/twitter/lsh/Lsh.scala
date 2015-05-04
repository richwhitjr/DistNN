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
 * @param radius - Expected "spread" of the vectors (used by HashFamily)
 * @param dimensions - # of dimensions in each vector (used by HashFamily)
 */
case class LshParams(hashTables: Int, hashFunctions: Int, radius: Double, dimensions: Int)

/**
 * See above. Note that two different types of Vectors are used:
 * Raw Vector - This is the vector which represents Item -> Vector
 *   In practice, Raw Vector could be a non-normalized DoubleLshVector or a (non-normalized)
 *   DecayedLshVector or some other type of vector.
 * Hash Vector - This is the normalized vector which is used for hashing.
 *   In practice, Hash Vector is probably DoubleLshVector and will be a normalized version
 *   of Raw Vector.
 * @param family - The type of hashing to use. Examples are Euclidean & Manhattan-distance based.
 * @param normalize - Function to convert Raw vector to Hash Vector
 * @param vectorStore - StoreHaus Store for storing the vectors.
 * @param hashTableManager - Manager for storing the hashes. Usually also a Storehaus Store
 * @tparam T - Item type to be stored
 * @tparam U - Vector type of Raw Vector
 * @tparam V - Vector type of Hash Vector
 */
class Lsh[T, U <: BaseLshVector, V <: BaseLshVector](family: HashFamily,
                                                     normalize: U => V,
                                                     vectorStore: MergeableStore[T, U],
                                                     hashTableManager: HashTableManager[T])
                                                    (implicit val uMonoid: Monoid[U]) {
  val log = Logger("Lsh")
  def update(keys: Set[T], value: U) = {
    // 1. Get current U vectors
    FutureOps.mapCollect(vectorStore.multiGet(keys))
      .onSuccess { vecs =>
        // 2. Calculate new U vectors
        val newVecs = vecs.mapValues(v => Some(if (v.isDefined) uMonoid.plus(v.get, value) else value))
        // 3. Put new U vectors
        FutureOps.mapCollect(vectorStore.multiPut(newVecs))
          .onSuccess { _ =>
            // 4. Calculate old Vs and delete
            val normOldVecs = vecs.filter(_._2.isDefined).mapValues(v => normalize(v.get))
            val normNewVecs = newVecs.mapValues(nv => (nv.get, normalize(nv.get)))
            hashTableManager.update(normOldVecs, normNewVecs)
          }
        }
  }
  /**
   * Given a normalized vector (V), returns matching keys (T) and their raw vectors (U).
   * Used internally by queryNormalizedVector but useful if raw vectors are wanted.
   * @param vector - Normalized vector of type V (usually LshVector)
   * @return Map[keyObject -> Option[rawVector]]
   */
  def queryNormalizedVectorRawResults(vector: V): Future[Map[T, Option[U]]] = {
    val vec: BaseLshVector = vector
    hashTableManager.query(Set(vec)).map{ candidates =>
      val (foundVecs, missingVecs) = candidates.partition { case (k, v) => v.isDefined}
      val mappedFoundVecs = foundVecs.mapValues{vec =>
        vec.get match {
        case u: U => Some(u)
        case _ => throw new ClassCastException
      }
    }
      log.debug("qNVRR.candidates.size: %s", candidates.size)
      log.debug("qNVRR.foundVecs.size: %s", foundVecs.size)
      log.debug("qnVRR.missingVecs.size: %s", missingVecs.size)
      if (missingVecs.nonEmpty) {
        val splitMissing = missingVecs.keySet.grouped(5000).toSeq
          .map(set => FutureOps.mapCollect(vectorStore.multiGet(set))(FutureCollector.bestEffort))

        Future.collect(splitMissing).map { list =>
          val obtained = list.reduce(_ ++ _)
          log.debug("qnVRR.obtained.size: %s", obtained.size)
          obtained ++ mappedFoundVecs
        }
      } else
        Future(mappedFoundVecs)
    }.flatten
  }

  /**
   * Given a normalized vector (V), returns matching keys (T) and their normalized vectors (V).
   * @param vector - Normalized vector of type V (usually DoubleLshVector)
   * @return (inputVector, Map[keyObject -> normalizedVector])
   */
  def queryNormalizedVector(vector: V): Future[(V, Map[T, V])] = {
    val futureCandidateVectorMap = queryNormalizedVectorRawResults(vector)

    futureCandidateVectorMap.map(candidateVectorMap =>
      (vector, candidateVectorMap.collect{case(k, Some(u)) => (k, normalize(u))}))
  }

  /**
   * Given a raw vector (U), returns matching keys (T) and their normalized vectors (V).
   * @param vector - Raw vector of type U
   * @return (inputVector, Map[keyObject -> normalizedVector])
   */
  def query(vector: U): Future[(U, Map[T, V])] = {
    log.info("vec: %s", vector.toDoubleVec.reduce(_+_))
    val normVec: V = normalize(vector)

    queryNormalizedVector(normVec).map(x => (vector, x._2))
  }

  /**
   * Given a key (T), returns matching keys (T) and their normalized vectors (V).
   * @param key - Key object of type T
   * @return ((inputKey, normalizedVector (if found)), Map[keyObject -> normalizedVector])
   */
  def query(key: T):Future[((T, Option[V]), Map[T, V])] = {
    vectorStore.get(key).flatMap { optKeyVec => optKeyVec.map { keyVec =>
      log.info("key: %s, vec: %s", key, keyVec.toDoubleVec.reduce(_ + _))
      query(keyVec).map { case (vec, map) => ((key, Some(normalize(vec))), map) }
    }.getOrElse(Future((key, None), Map[T, V]()))}
  }

  // This helper method is generally not for public use. It is used internally and by tests.
  def scoreQuery(vector: V, candidateMap: Map[T, V], maxResults:Int) = {
    log.info("scoreQuery candidateMap.size() = %s", candidateMap.size)
    candidateMap.mapValues{vec => family.score(vector, vec)}
      .toList
      .sortBy(_._2)
      .reverse
      .take(maxResults)
  }

  /**
   * Given a key (T), returns the top N matches & scores, based on hash family scoring
   * @param key - Key object of type T
   * @param maxResults - max # of matches to return
   * @return - Matching keys and their scores in descending score order.
   */
  def scoredQuery(key: T, maxResults: Int):Future[List[(T, Double)]] = {
    query(key).map{case ((key, keyVec), candidateMap) =>
      if (keyVec.isDefined)
        scoreQuery(keyVec.get, candidateMap, maxResults)
      else
        Nil
    }
  }

  /**
   * Given a normalized vector (V), returns top N matches & scores, based on hash family scoring
   * @param vector - Normalized vector of type V
   * @param maxResults - max # of matches to return
   * @return - Matching keys and their scores in descending score order.
   */
  def scoredNormalizedVector(vector: V, maxResults: Int):Future[List[(T, Double)]] = {
    queryNormalizedVector(vector).map{case (keyVec, candidateMap) =>
        scoreQuery(keyVec, candidateMap, maxResults)
    }
  }
}
