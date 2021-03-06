package com.twitter.lsh.hashing

import com.twitter.logging.Logger
import com.twitter.lsh.vector.{BaseLshVector, VectorMath}

import scala.util.Random

class EuclideanHashFamily(radius: Double, dimension: Int) extends HashFamily with Serializable {
  override val familyId = 1
  override def hashCode = dimension*1000+(radius*10).toInt+familyId

  class EuclideanHasher(hashTableId: Int, hashFunctionId: Int, radius: Double, dimension: Int) extends Hasher with Serializable {
    lazy val log = Logger("Euclidean Hasher")

    val rand = randomGenerator(hashTableId*10000 + hashFunctionId)

    val offset = rand.nextInt(radius.toInt)
    val randomProjection = Array.ofDim[Double](dimension)
    for (i <- 0 until dimension) {
      randomProjection.update(i, rand.nextGaussian)
    }

    def hash(vector: Array[Double]): Int = {
      scala.math.round((VectorMath.dot(vector, randomProjection)+offset)/radius).toInt
    }
  }

  def createHasher(hashTableId: Int, hashFunctionId: Int): Hasher = {
    new EuclideanHasher(hashTableId, hashFunctionId, radius, dimension)
  }

  def combine(hashes: Array[Int]): Int = java.util.Arrays.hashCode(hashes)

  def score[U <: BaseLshVector[U], T <: BaseLshVector[T]](keyVec: U, candidateVec: T): Double =
    VectorMath.euclideanDistance(keyVec, candidateVec)
}
