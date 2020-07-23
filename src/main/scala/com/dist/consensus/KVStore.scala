package com.dist.consensus

import java.io.{ByteArrayInputStream, File}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import java.util

case class ClientSession(lastModifiedTime: Long, clientId: String, responses: util.Map[Int, String]) {

}

class KVStore(walDir: File) {
  val kv = new mutable.HashMap[String, String]()
  val wal = WriteAheadLog.create(walDir)
  applyLog()

  def put(key: String, value: String): Unit = {
    wal.writeEntry(SetValueCommand(key, value).serialize())
  }

  def get(key: String): Option[String] = kv.get(key)

  def close = {
    kv.clear()
  }


  def applyEntries(entries: List[WalEntry]): Unit = {
    entries.foreach(entry ⇒ {
      applyEntry(entry)
    })
  }


  val sessions = new util.HashMap[String, ClientSession]

  def applyEntry(entry: WalEntry) = {
    if (entry.entryType == EntryType.data) {
      val command = Command.deserialize(new ByteArrayInputStream(entry.data))
      command match {
        case command: SetValueCommand => {
          if (command.clientId.isEmpty) {
            kv.put(command.key, command.value)
          } else {
            mayBeRespondFromSession(entry, command)
          }
        }
        case command: RegisterClientCommand => {
          val clientId = if (command.clientId.isEmpty) s"${entry.entryId}" else command.clientId
          sessions.put(clientId, new ClientSession(entry.leaderTime, clientId, new util.HashMap[Int, String]()))
          clientId
        }
      }
    }
  }

  private def mayBeRespondFromSession(entry: WalEntry, command: SetValueCommand): String = {
    val session = sessions.get(command.clientId)
    if (session == null) {
      throw new IllegalStateException("No client session found")
    }

    val responses = session.responses
    val value: String = responses.get(command.sequenceNo)
    if (value == null) {
      kv.put(command.key, command.value)
      responses.put(command.sequenceNo, command.value)
      sessions.put(command.clientId, session.copy(lastModifiedTime = entry.leaderTime))
      command.value
    } else value
  }

  def applyLog() = {
    val entries: List[WalEntry] = wal.readAll().toList
    applyEntries(entries)
  }
}
