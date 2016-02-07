package com.twitter.lsh.hashing

import com.twitter.logging.Logger
import com.twitter.lsh.vector.{BaseLshVector, VectorMath}

class CosineHashFamily(dimension: Int) extends HashFamily with Serializable {
  override val familyId = 2
  override def hashCode = dimension*1000+familyId

  class CosineHasher(hashTableId: Int, hashFunctionId: Int, dimension: Int) extends Hasher with Serializable {
    lazy val log = Logger("Cosine Hasher")

    val rand = randomGenerator(hashTableId*10000 + hashFunctionId)

    val randomProjection = Array.ofDim[Double](dimension)

    for (i <- 0 until dimension)
      randomProjection.update(i, rand.nextGaussian)

    def hash(vector: Array[Double]): Int =
      if(VectorMath.dot(vector, randomProjection) < 0) 1 else 0
  }

  def createHasher(hashTableId: Int, hashFunctionId: Int): Hasher =
    new CosineHasher(hashTableId, hashFunctionId, dimension)

  def combine(hashes: Array[Int]): Int =
    hashes.foldLeft(0, 1){case((result, factor), hash) =>
      val result = if(hash == 0) 0 else factor
      (result, factor * 2)
    }._1

  def score[U <: BaseLshVector[U], T <: BaseLshVector[T]](keyVec: U, candidateVec: T): Double =
    1.0 - VectorMath.cosineDistance(keyVec, candidateVec)
}
