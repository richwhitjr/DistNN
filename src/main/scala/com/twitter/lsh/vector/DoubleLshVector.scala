package com.twitter.lsh.vector

import com.twitter.algebird.Monoid
import com.twitter.bijection.{Bijection, Bufferable}

/**
 * Defines the Injection and Monoid necessary for a MergeableSource based on DoubleLshVector
 */
object DoubleLshVector {
  // DoubleLshVector <=> Array[Double]
  implicit val doubleLshVectorBijection = Bijection.build { dlv:DoubleLshVector => dlv.toDoubleVec }
  { case vec => DoubleLshVector(vec) }

  // DoubleLshVector => Array[Double] => Array[Bytes]
  implicit val doubleLshVectorInjection =
    Bufferable.injectionOf(Bufferable.viaBijection[DoubleLshVector, Array[Double]])

  implicit val doubleLshVectorMonoid = new Monoid[DoubleLshVector] {
    val saMonoid = new SummingArrayMonoid[Double]()
    override def zero = DoubleLshVector(saMonoid.zero)
    override def plus(left: DoubleLshVector, right: DoubleLshVector) =
      DoubleLshVector(saMonoid.plus(left.vector, right.vector))
  }
}

/**
 * DoubleLshVector is just a thin wrapper around an Array of Doubles. It's pretty much the most
 * basic LshVector possible.
 * @param vector - The Array of Doubles to wrap.
 */
case class DoubleLshVector(vector: Array[Double]) extends LshVector {
  def size = vector.size
  def apply(index:Int) = vector(index)
  def toDoubleVec = vector
}
