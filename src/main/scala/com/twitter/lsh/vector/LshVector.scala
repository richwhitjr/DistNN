package com.twitter.lsh.vector

import com.twitter.algebird._
import com.twitter.bijection.{Bijection, Bufferable}
import java.util

/**
 * Defines the necessary operations to have a vector for LSH as well as 2 vector types.
 */

/**
 * An LshVector must permit the operations listed below.
 */
trait LshVector {
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

/**
 * Defines the Injection and builder for the Monoid necessary for a MergeableSource
 * based on DecayedLshVector.
 * Since decayed vectors can have variable epsilons, call DecayedLshVector.monoidWithEpsilon() to
 * create an implicit monoid for DecayedLshVectors.
 */
object DecayedLshVector {
  // DecayedLshVector <=> DecayedVector
  implicit val decayedLshVectorBijection = Bijection.build { dlv:DecayedLshVector => dlv.vector }
  { case vec => new DecayedLshVector(vec) }

  // DecayedVector <=> (Array[Double], Double)
  implicit val decayedVectorBijection = Bijection.build { dv: DecayedVector[Array] =>
    (dv.vector, dv.scaledTime) } { case (vec, time) => DecayedVector[Array](vec, time) }

  // (Array[Double], Double) => Array[Bytes]
  implicit val tupleInjection = Bufferable.injectionOf[(Array[Double], Double)]

  // DecayedVector => (Array[Double, Double) => Array[Bytes]
  implicit val decayedVectorBufferable =
    Bufferable.viaBijection[DecayedVector[Array], (Array[Double], Double)]

  // DecayedLshVector => DecayedVector( => ...) => Array[Bytes]
  implicit val decayedLshVectorInjection =
    Bufferable.injectionOf(Bufferable.viaBijection[DecayedLshVector, DecayedVector[Array]])

  // The following is for the Monoid.
  implicit def doubleArrayGroup = new SummingArrayGroup[Double]

  implicit def doubleArraySpace =
    VectorSpace.from[Double, Array]{(s, array) => array.map(Ring.times(s, _)).toArray}

  // TODO(acs): I'm not sure how inefficient this is. If it is, define metric in terms of Array.
  implicit def doubleArrayMetric = new Metric[Array[Double]] {
    val iDMetric = Metric.iterableMetric[Double]
    def apply(v1: Array[Double], v2: Array[Double]) = iDMetric.apply(v1.toSeq, v2.toSeq)
  }

  /**
   * Returns a monoid used to add decayed lsh vectors together.
   * @param epsilon - epsilon for monoid
   * @return - Monoid for DecayedLshVectors.
   */
  def monoidWithEpsilon(epsilon: Double) = {
    val dvMonoid = DecayedVector.monoidWithEpsilon[Array](epsilon)

    new Monoid[DecayedLshVector] {
      override def zero = new DecayedLshVector(dvMonoid.zero)
      override def plus(left: DecayedLshVector, right: DecayedLshVector) = {
        new DecayedLshVector(dvMonoid.plus(left.vector, right.vector))
      }
    }
  }

  /**
   * extractor can be used to convert a DecayedVector into an array of the vector decayed to the
   * provided time.
   * For example:
   *   def myExtractor = extractor(90.minutes.inMillis)
   *   myExtractor(myDecayedLshVector) will give myDecayedLshVector's value at the current time.
   *
   * @param halfLife - half life (in ms) for decay
   * @param time - time to decay vector to
   * @param decayedVectorMonoid - Monoid to sum vectors with
   * @return - Array[Double] of decayed coefficients
   */
  def extractor(halfLife: Long, time: Long = System.currentTimeMillis,
                decayedVectorMonoid: Monoid[DecayedLshVector] = monoidWithEpsilon(-1.0)):
  DecayedLshVector => Array[Double] = {
    (vector: DecayedLshVector) => {
      decayedVectorMonoid.plus(vector,
        new DecayedLshVector(Array.ofDim[Double](vector.vector.vector.size), time, halfLife)).toDoubleVec
    }
  }
}

/**
 * Wraps algebird's DecayedVector to allow Lsh for exponential moving averages.
 * @param in - DecayedVector to wrap. In practice, consumers will use the internal constructors.
 */
class DecayedLshVector(in: DecayedVector[Array]) extends LshVector {
  // Converts Array + scaledTime to wrapped DecayedVector. Used by Injections/Bijections
  def this(in: Array[Double], scaledTime: Double) = this(DecayedVector[Array](in, scaledTime))
  // Converts Array + time + halflife into a wrapped DecayedVector. This is the constructor usually
  // used by Lsh consumers.
  def this(in: Array[Double], time: Double, halfLife: Double) =
    this(DecayedVector.buildWithHalflife(in, time, halfLife))

  val vector = in
  override def size = vector.vector.size
  override def apply(index:Int) = vector.vector(index)
  override def toDoubleVec = vector.vector
}
