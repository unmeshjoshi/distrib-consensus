package com.dist.consensus

import org.scalatest.FunSuite

class KVStoreTest extends FunSuite {
  test("should have values restored after a crash/shutdown") {
    val walDir = TestUtils.tempDir("waltest")
    val kv = new KVStore(walDir)

    kv.put("k1", "v1")
    kv.put("k2", "v2")
    kv.close

    val restartedKv = new KVStore(walDir)
    assert(Some("v2") == restartedKv.get("k2"))
    assert(Some("v1") == restartedKv.get("k1"))
  }

}
