package com.twitter.lsh.vector

import com.twitter.algebird._
import com.twitter.bijection.{Bijection, Bufferable}

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
class DecayedLshVector(in: DecayedVector[Array]) extends BaseLshVector {
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
