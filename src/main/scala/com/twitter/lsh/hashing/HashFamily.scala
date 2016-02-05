package com.twitter.lsh.hashing

import com.twitter.lsh.vector.BaseLshVector

trait Hasher {
  def hash(vector: Array[Double]): Int
}

trait HashFamily {
  val familyId = -1  // Should be unique to each Family.
  def createHasher(hashTableId: Int, hashFunctionId: Int): Hasher
  def combine(hashes: Array[Int]): Int
  def score[U <: BaseLshVector[U], T <: BaseLshVector[T]](keyVec: U, candidateVec: T): Double
}
