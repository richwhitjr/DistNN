package com.twitter.lsh.hashing

import com.twitter.lsh.vector.BaseLshVector

import scala.util.Random

trait Hasher {
  def hash(vector: Array[Double]): Int
}

trait HashFamily {
  def randomGenerator(seed: Int) = {
    val rand = new Random(seed)
    //http://stackoverflow.com/questions/12282628/why-are-initial-random-numbers-similar-when-using-similar-seeds
    rand.nextInt()
    rand
  }
  val familyId = -1  // Should be unique to each Family.
  def createHasher(hashTableId: Int, hashFunctionId: Int): Hasher
  def combine(hashes: Array[Int]): Int
  def score[U <: BaseLshVector[U], T <: BaseLshVector[T]](keyVec: U, candidateVec: T): Double
}
