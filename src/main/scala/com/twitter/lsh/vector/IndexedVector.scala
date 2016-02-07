package com.twitter.lsh.vector

//Utility class to help pass around vector with it's index
case class IndexedVector[B <: BaseLshVector[B]](id: Long, vector: B)
