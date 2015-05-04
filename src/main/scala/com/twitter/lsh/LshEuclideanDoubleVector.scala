package com.twitter.lsh

import com.twitter.lsh.hashing.EuclideanHashFamily
import com.twitter.lsh.stores.{HashTableManagerMemory, VectorStoreMemory}
import com.twitter.lsh.vector.{LshVector, VectorMath}

case class IndexedVector(id:Long, vector:LshVector)

object LshEuclideanDoubleVector {
  def withVectors(lshParams:LshParams, vectors:Array[IndexedVector]) = {
    val lsh = apply(lshParams)
    vectors.foreach{vec => lsh.update(Set(vec.id), vec.vector)}
    lsh
  }
  def apply(lshParams:LshParams) = {
    implicit val monoid = LshVector.lshVectorMonoid
    val hashFamily = new EuclideanHashFamily(lshParams.radius, lshParams.dimensions)
    val vectorStore = VectorStoreMemory[Long, LshVector]
    val hashTableManager = new HashTableManagerMemory[Long](hashFamily, lshParams.hashTables, lshParams.hashFunctions)
    new Lsh[Long, LshVector, LshVector](hashFamily, VectorMath.normalize, vectorStore, hashTableManager)
  }
}
