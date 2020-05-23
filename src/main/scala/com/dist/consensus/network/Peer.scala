package com.dist.consensus.network

case class Peer(id:Int, address:InetAddressAndPort)


case class PeerProxy(peerInfo: Peer, var matchIndex: Long = 0, heartbeatSender: PeerProxy ⇒ Unit) {
  def heartbeatSenderWrapper() = {
    heartbeatSender(this)
  }

  def start(): Unit = {

  }

  def stop()= {

  }
}
