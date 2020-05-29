package com.dist.consensus

import java.io.{ByteArrayOutputStream, DataInputStream, DataOutputStream, InputStream}

trait Command {
  def serialize():Array[Byte]
}


object SetValueCommand {
  def deserialize(is:InputStream) = {
    val daos = new DataInputStream(is)
    val key = daos.readUTF()
    val value = daos.readUTF()
    SetValueCommand(key, value)
  }
}

case class SetValueCommand(val key:String, val value:String) extends Command {
  def serialize() = {
    val baos = new ByteArrayOutputStream
    val dataStream = new DataOutputStream(baos)
    dataStream.writeUTF(key)
    dataStream.writeUTF(value)
    baos.toByteArray
  }
}

object RegisterClientCommand {
  def deserialize(is:InputStream) = {
    val daos = new DataInputStream(is)
    val clientId = daos.readUTF()
    RegisterClientCommand(clientId)
  }
}

case class RegisterClientCommand(val clientId:String) extends Command {
  override def serialize(): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val dataStream = new DataOutputStream(baos)
    dataStream.writeUTF(clientId)
    baos.toByteArray
  }
}
