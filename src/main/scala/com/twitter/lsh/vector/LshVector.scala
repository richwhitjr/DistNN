package com.twitter.lsh.vector

import com.twitter.algebird.Monoid
import com.twitter.bijection.{Bijection, Bufferable}

/**
 * Defines the Injection and Monoid necessary for a MergeableSource based on DoubleLshVector
 */
object LshVector {
  // DoubleLshVector <=> Array[Double]
  implicit val lshVectorBijection = Bijection.build { dlv:LshVector => dlv.toDoubleVec }
  { case vec => LshVector(vec) }

  // DoubleLshVector => Array[Double] => Array[Bytes]
  implicit val lshVectorInjection =
    Bufferable.injectionOf(Bufferable.viaBijection[LshVector, Array[Double]])

  implicit val lshVectorMonoid = new Monoid[LshVector] {
    val saMonoid = new SummingArrayMonoid[Double]()
    override def zero = LshVector(saMonoid.zero)
    override def plus(left: LshVector, right: LshVector) = LshVector(saMonoid.plus(left.vector, right.vector))
  }
}

/**
 * DoubleLshVector is just a thin wrapper around an Array of Doubles. It's pretty much the most
 * basic LshVector possible.
 * @param vector - The Array of Doubles to wrap.
 */
case class LshVector(vector: Array[Double]) extends BaseLshVector {
  def size = vector.size
  def apply(index:Int) = vector(index)
  def toDoubleVec = vector
}
