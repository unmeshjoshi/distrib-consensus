package com.dist.consensus

import scala.collection.mutable

class KVStore {
  val kv = new mutable.HashMap[String, String]()

  def put(key:String, value:String): Unit = {
    kv.put(key, value)
  }

  def get(key: String): Option[String] = kv.get(key)

  def close = {
    kv.clear()
  }
}
