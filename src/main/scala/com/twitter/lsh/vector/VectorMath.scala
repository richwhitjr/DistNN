package com.twitter.lsh.vector

object VectorMath {
  def divide[R <: BaseLshVector[R]](vector: R, scalar: Double): R = {
    val returnVec = Array.ofDim[Double](vector.size)

    var idx = 0
    while(idx < vector.size){
      returnVec(idx) = vector(idx) / scalar
      idx += 1
    }

    vector.vectorCopy(returnVec)
  }

  def multiply[R <: BaseLshVector[R]](vector: R, scalar: Double): R = {
    val returnVec = Array.ofDim[Double](vector.size)
    var idx = 0

    while(idx < vector.size){
      returnVec(idx) = vector(idx) * scalar
      idx += 1
    }

    vector.vectorCopy(returnVec)
  }

  def vectorDivide[R <: BaseLshVector[R], S <: BaseLshVector[S]](vector: R, vector2: S): R = {
    val minSize = math.min(vector.size, vector2.size)

    val returnVec = Array.ofDim[Double](minSize)
    var idx = 0

    while(idx < minSize){
      returnVec(idx) = if(vector2(idx) == 0.0) 0.0 else vector(idx) / vector2(idx)
      idx += 1
    }

    vector.vectorCopy(returnVec)
  }

  def vectorMultiply[R <: BaseLshVector[R], S <: BaseLshVector[S]](vector: R, vector2: S): R = {
    val minSize = math.min(vector.size, vector2.size)
    val returnVec = Array.ofDim[Double](minSize)
    var idx = 0

    while(idx < minSize){
      returnVec(idx) = vector(idx) * vector2(idx)
      idx += 1
    }

    vector.vectorCopy(returnVec)
  }

  def vectorSum[R <: BaseLshVector[R], S <: BaseLshVector[S]](vector: R, vector2: S): R = {
    val minSize = math.min(vector.size, vector2.size)

    val returnVec = Array.ofDim[Double](minSize)
    var idx = 0

    while(idx < minSize){
      returnVec(idx) = vector(idx) + vector2(idx)
      idx += 1
    }

    vector.vectorCopy(returnVec)
  }

  def cosineDistance[R <: BaseLshVector[R], S <: BaseLshVector[S]](vector: R, vector2: S): Double =
    vectorDot(vector, vector2) / math.sqrt(vectorDot(vector, vector) * vectorDot(vector2, vector2))

  def euclideanDistance[R <: BaseLshVector[R], S <: BaseLshVector[S]](vector: R, vector2: S): Double = {
    val minSize = math.min(vector.size, vector2.size)

    val returnVec = Array.ofDim[Double](minSize)
    var idx = 0
    var sum = 0.0

    while(idx < minSize){
      val value = vector(idx) - vector2(idx)
      sum += value * value
      idx += 1
    }

    math.sqrt(sum)
  }

  def dot(v: Array[Double], v2: Array[Double]): Double = {
    val minSize = math.min(v.length, v2.length)

    var sum = 0.0
    var idx = 0

    while(idx < minSize){
      val times = v(idx) * v2(idx)
      sum += times
      idx += 1
    }

    sum
  }

  def vectorDot[R <: BaseLshVector[R], S <: BaseLshVector[S]](vector: R, vector2: S): Double =
    dot(vector.vector, vector2.vector)

  def magnitude[R](vec: BaseLshVector[R]): Double = {
    var sum = 0.0
    var idx = 0

    while(idx < vec.size){
      sum += vec(idx) * vec(idx)
      idx += 1
    }

    math.sqrt(sum)
  }

  def normalize[R <: BaseLshVector[R]](vector: R): R =
    divide(vector, magnitude(vector))

  def identity[R <: BaseLshVector[R]](vector: R): R = vector
}
