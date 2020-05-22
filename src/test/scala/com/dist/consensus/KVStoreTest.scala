package com.dist.consensus

import org.scalatest.FunSuite

class KVStoreTest extends FunSuite {
  test("should have values restored after a crash/shutdown") {
    val kv = new KVStore()
    kv.put("k1", "v1")
    kv.put("k2", "v2")
    kv.close

    val restartedKv = new KVStore()
    assert("v2" == restartedKv.get("k2"))
    assert("v1" == restartedKv.get("k1"))
  }

}
