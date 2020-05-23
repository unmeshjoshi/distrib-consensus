package com.dist.consensus

import java.io.File
import java.util.concurrent.atomic.AtomicReference

import com.dist.consensus.election.{RequestKeys, Vote, VoteResponse}
import com.dist.consensus.network.{Config, InetAddressAndPort, JsonSerDes, Peer, RequestOrResponse, TcpListener}

object ServerState extends Enumeration {
  type ServerState = Value
  val LOOKING, FOLLOWING, LEADING = Value
}

class Server(config: Config) extends Thread with Logging {
  val kv = new KVStore(config.walDir)


  @volatile var running = true
  private val client = new NetworkClient()


  def requestHandler(request: RequestOrResponse) = {
    if (request.requestId == RequestKeys.RequestVoteKey) {
      val vote = VoteResponse(currentVote.get().id, currentVote.get().zxid)
      info(s"Responding vote response from ${config.serverId} be ${currentVote}")
      RequestOrResponse(RequestKeys.RequestVoteKey, JsonSerDes.serialize(vote), request.correlationId)
    }
    else throw new RuntimeException("UnknownRequest")

  }

  def startListening() = new TcpListener(config.serverAddress, requestHandler).start()

  val currentVote = new AtomicReference(Vote(config.serverId, kv.wal.lastLogEntryId))

  @volatile var state: ServerState.Value = ServerState.LOOKING

  def setPeerState(serverState: ServerState.Value) = this.state = serverState

  override def run() = {
    while (true) {
      if (state == ServerState.LOOKING) {
        try {
          val electionResult = new LeaderElector(config, this, config.getPeers()).lookForLeader()
        } catch {
          case e: Exception ⇒ {
            e.printStackTrace()
            state = ServerState.LOOKING
          }
        }
      }
    }
  }
}
