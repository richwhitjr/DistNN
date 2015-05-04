package com.twitter.lsh

import com.twitter.lsh.hashing.EuclideanHashFamily
import com.twitter.lsh.stores.{HashTableManagerMemory, VectorStoreMemory}
import com.twitter.lsh.vector.{LshVector, VectorMath, DoubleLshVector}

case class IndexedVector(id:Long, vector:DoubleLshVector)

object LshEuclideanDoubleVector {
  def withVectors(lshParams:LshParams, vectors:Array[IndexedVector]) = {
    val lsh = apply(lshParams)
    vectors.foreach{vec => lsh.update(Set(vec.id), vec.vector)}
    lsh
  }
  def apply(lshParams:LshParams) = {
    implicit val monoid = DoubleLshVector.doubleLshVectorMonoid
    val hashFamily = new EuclideanHashFamily(lshParams.radius, lshParams.dimensions)
    val vectorStore = VectorStoreMemory[Long, DoubleLshVector]
    val hashTableManager = new HashTableManagerMemory[Long](hashFamily, lshParams.hashTables, lshParams.hashFunctions)
    new Lsh[Long, DoubleLshVector, DoubleLshVector](hashFamily, VectorMath.normalize, vectorStore, hashTableManager)
  }
}
