package com.twitter.lsh.vector

import java.util

import com.twitter.algebird._

/**
 * An LshVector must permit the operations listed below.
 */
trait BaseLshVector {
  def size: Int
  def apply(index: Int): Double  // Return Vector[index]
  def toDoubleVec: Array[Double]
  override def hashCode:Int = util.Arrays.hashCode(toDoubleVec)
}

/**
 * Monoid defining Array1 + Array2 as the pairwise sum of each entry.
 * @tparam T - Type of items being summed. This is usually Doubles for lsh purposes.
 */
class SummingArrayMonoid[T](implicit semi: Semigroup[T], manifest: Manifest[T])
  extends Monoid[Array[T]] {
  override def zero = Array[T]()
  override def plus(left: Array[T], right: Array[T]) = {
    val (longer, shorter) = if (left.length > right.length) (left, right) else (right, left)
    val sum = longer.clone
    for (i <- 0 until shorter.length)
      sum.update(i, semi.plus(sum(i), shorter(i)))

    sum
  }
}

/**
 * Extends the previous Monoid into a Group by providing negation. This is necessary for Monoids for
 * decayed vectors.
 * @tparam T - Type of items being summed. This is usually Doubles for lsh purposes.
 */
class SummingArrayGroup[T](implicit grp: Group[T], manifest: Manifest[T])
  extends SummingArrayMonoid[T]()(grp, manifest) with Group[Array[T]] {
  override def negate(g: Array[T]): Array[T] = g.map{ grp.negate(_)}.toArray
}
