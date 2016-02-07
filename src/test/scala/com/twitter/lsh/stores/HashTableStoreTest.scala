package com.twitter.lsh.stores

import com.twitter.lsh.hashing.CosineHashFamily
import com.twitter.lsh.vector.LshVector
import com.twitter.util.Await
import org.scalatest.{Matchers, WordSpec}

class HashTableStoreTest extends WordSpec with Matchers {
  "Hash Table Store" should {
    val vector = LshVector(Array(1.0))
    val hashFamily = new CosineHashFamily(1)
    val manager = new HashTableManagerMemory[Long, LshVector](hashFamily, 1, 1)

    "update and return value" in {
      val map = Map(1L -> vector)
      Await.result(manager.update(Map.empty, map))

      val result = Await.result(manager.query(Set(vector)))
      assert(result.keySet.contains(1L))

      Await.result(manager.update(map, Map.empty))

      val result2 = Await.result(manager.query(Set(vector)))
      assert(result2.keySet.size === 0)
    }
  }
}
