package com.twitter.lsh.stores

import com.twitter.algebird.Monoid
import com.twitter.lsh.hashing.HashFamily
import com.twitter.lsh.vector._
import com.twitter.storehaus.JMapStore
import com.twitter.storehaus.algebra.MergeableStore

/**
 * In memory implementation of LSH storage using Java Map
 */
class HashTableManagerMemory[T](family: HashFamily, numHashTables: Int, numHashes: Int)
  extends HashTableManagerStore[T](family, numHashTables, numHashes){
  override val hashTable = MergeableStore.fromStore(new JMapStore[(TableIdentifier, Int), Set[T]])
}

object VectorStoreMemory {
  def apply[T, U <: LshVector](implicit arrayMonoid: Monoid[U]): MergeableStore[T, U] = {
    MergeableStore.fromStore(new JMapStore[T, U])
  }
}

