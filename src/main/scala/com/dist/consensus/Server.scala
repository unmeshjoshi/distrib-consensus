package com.dist.consensus

import java.io.{ByteArrayInputStream, File}
import java.util.concurrent.atomic.AtomicReference

import com.dist.consensus.election.{RequestKeys, Vote, VoteResponse}
import com.dist.consensus.network.{Config, InetAddressAndPort, JsonSerDes, Peer, PeerProxy, RequestOrResponse, TcpListener}

import scala.collection.mutable.ListBuffer

object ServerState extends Enumeration {
  type ServerState = Value
  val LOOKING, FOLLOWING, LEADING = Value
}

class Server(config: Config) extends Thread with Logging {
  def shutdown(): Any = {
    leader.followerProxies.foreach(p => p.stop())

  }

  var commitIndex = 0L

  var leader:Leader = _

  val kv = new KVStore(config.walDir)
  val electionTimeoutChecker = new HeartBeatScheduler(heartBeatCheck)

  def handleHeartBeatTimeout() = {
    info(s"Heartbeat timeout starting election in ${config.serverId}")
    this.state == ServerState.LOOKING
  }

  var heartBeatReceived = false
  def heartBeatCheck() = {
    info(s"Checking if heartbeat received in ${state} ${config.serverId}")
    if(!heartBeatReceived) {
      handleHeartBeatTimeout()
    } else {
      heartBeatReceived = false //reset
    }
  }

  @volatile var running = true
  private val client = new NetworkClient()


  def handleAppendEntries(appendEntryRequest: AppendEntriesRequest) = {
    heartBeatReceived = true

    val lastLogEntry = this.kv.wal.lastLogEntryId
    if (appendEntryRequest.data.size == 0) { //this is heartbeat
      updateCommitIndex(appendEntryRequest)
      AppendEntriesResponse(lastLogEntry, true)

    } else if (lastLogEntry >= appendEntryRequest.xid) {
      AppendEntriesResponse(lastLogEntry, false)

    } else {
      this.kv.wal.writeEntry(appendEntryRequest.data)
      updateCommitIndex(appendEntryRequest)
    }
  }

  private def updateCommitIndex(appendEntryRequest: AppendEntriesRequest) = {
    if (this.commitIndex < appendEntryRequest.commitIndex) {
      updateCommitIndexAndApplyEntries(appendEntryRequest.commitIndex)
    }
    AppendEntriesResponse(this.kv.wal.lastLogEntryId, true)
  }

  def requestHandler(request: RequestOrResponse) = {
    if (request.requestId == RequestKeys.RequestVoteKey) {
      val vote = VoteResponse(currentVote.get().id, currentVote.get().lastLogIndex)
      info(s"Responding vote response from ${config.serverId} be ${currentVote}")
      RequestOrResponse(RequestKeys.RequestVoteKey, JsonSerDes.serialize(vote), request.correlationId)
    } else if (request.requestId == RequestKeys.AppendEntriesKey) {

      val appendEntries = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[AppendEntriesRequest])
      val appendEntriesResponse = handleAppendEntries(appendEntries)
      info(s"Responding AppendEntriesResponse from ${config.serverId} be ${appendEntriesResponse}")
      RequestOrResponse(RequestKeys.AppendEntriesKey, JsonSerDes.serialize(appendEntriesResponse), request.correlationId)

    }
    else throw new RuntimeException("UnknownRequest")
  }

  def applyEntries(entries: ListBuffer[WalEntry]) = {
    kv.applyEntries(entries.toList)
  }

  def updateCommitIndexAndApplyEntries(index:Long) = {
    val previousCommitIndex = commitIndex
    commitIndex = index
    kv.wal.highWaterMark = commitIndex
    info(s"Applying wal entries in ${config.serverId} from ${previousCommitIndex} to ${commitIndex}")
    val entries = kv.wal.entries(previousCommitIndex, commitIndex)
    applyEntries(entries)
  }

  val listener = new TcpListener(config.serverAddress, requestHandler)
  def startListening() = {
    listener.start()
  }

  val currentVote = new AtomicReference(Vote(config.serverId, kv.wal.lastLogEntryId))

  @volatile var state: ServerState.Value = ServerState.LOOKING

  def setPeerState(serverState: ServerState.Value) = this.state = serverState

  def put(key:String, value:String) = {
    if (leader == null) throw new RuntimeException("Can not propose to non leader")

    //propose is synchronous as of now so value will be applied
    leader.propose(SetValueCommand(key, value))

    kv.get(key)
  }

  def get(key:String) = {
    kv.get(key)
  }


  override def run() = {
    while (true) {
      if (state == ServerState.LOOKING) {
        try {
          val electionResult = new LeaderElector(config, this, config.getPeers()).lookForLeader()
          electionTimeoutChecker.cancel()
        } catch {
          case e: Exception ⇒ {
            e.printStackTrace()
            state = ServerState.LOOKING
          }
        }
      } else if (state == ServerState.LEADING) {
        electionTimeoutChecker.cancel()
        this.leader = new Leader(config, new NetworkClient(), this)
        this.leader.startLeading()

      } else if (state == ServerState.FOLLOWING) {
        electionTimeoutChecker.cancel()
        electionTimeoutChecker.startWithRandomInterval()
        startFollowing()
      }
    }

    def startFollowing(): Unit = {
      while(this.state == ServerState.FOLLOWING) {
        Thread.sleep(100)
      }
      info(s"Giving up follower state in ${config.serverId}")
    }
  }
}


case class AppendEntriesRequest(xid:Long, data:Array[Byte], commitIndex:Long)
case class AppendEntriesResponse(xid:Long, success:Boolean)

class Leader(config:Config, client:NetworkClient, val self:Server) extends Logging {
  val followerProxies = config.getPeers().map(p ⇒ PeerProxy(p, 0, sendHeartBeat))
  def startLeading() = {
    followerProxies.foreach(_.start())
    while(self.state == ServerState.LEADING) {
      Thread.sleep(100)
    }
  }

  def sendHeartBeat(peerProxy:PeerProxy) = {
    val appendEntries = JsonSerDes.serialize(AppendEntriesRequest(0, Array[Byte](), self.kv.wal.highWaterMark))
    val request = RequestOrResponse(RequestKeys.AppendEntriesKey, appendEntries, 0)
    //sendHeartBeat
    val response = client.sendReceive(request, peerProxy.peerInfo.address)
    //TODO: Handle response
    val appendOnlyResponse: AppendEntriesResponse = JsonSerDes.deserialize(response.messageBodyJson.getBytes(), classOf[AppendEntriesResponse])
    if (appendOnlyResponse.success) {
      peerProxy.matchIndex = appendOnlyResponse.xid
    } else {
      // TODO: handle term and failures
    }
  }

  def stopLeading() = followerProxies.foreach(_.stop())

  var lastEntryId: Long = self.kv.wal.lastLogEntryId


  def propose(setValueCommand:SetValueCommand) = {
    val data = setValueCommand.serialize()

    appendToLocalLog(data)

    broadCastAppendEntries(data)
  }

  private def findMaxIndexWithQuorum = {
    val matchIndexes = followerProxies.map(p ⇒ p.matchIndex)
    val sorted: Seq[Long] = matchIndexes.sorted
    val matchIndexAtQuorum = sorted((config.peerConfig.size - 1) / 2)
    matchIndexAtQuorum
  }

  private def broadCastAppendEntries(data: Array[Byte]) = {
    val request = appendEntriesRequestFor(data)

    //TODO: Happens synchronously for demo. Has to be async with each peer having its own thread
    followerProxies.map(peer ⇒ {
      val response = client.sendReceive(request, peer.peerInfo.address)
      val appendEntriesResponse = JsonSerDes.deserialize(response.messageBodyJson.getBytes(), classOf[AppendEntriesResponse])

      peer.matchIndex = appendEntriesResponse.xid

      val matchIndexAtQuorum = findMaxIndexWithQuorum


      info(s"Peer match indexes are at ${config.peerConfig}")
      info(s"CommitIndex from quorum is ${matchIndexAtQuorum}")

      if (self.commitIndex < matchIndexAtQuorum) {
        self.updateCommitIndexAndApplyEntries(matchIndexAtQuorum)
      }

    })
  }

  private def appendEntriesRequestFor(data: Array[Byte]) = {
    val appendEntries = JsonSerDes.serialize(AppendEntriesRequest(lastEntryId, data, self.commitIndex))
    val request = RequestOrResponse(RequestKeys.AppendEntriesKey, appendEntries, 0)
    request
  }

  private def appendToLocalLog(data:Array[Byte]) = {
    lastEntryId = self.kv.wal.writeEntry(data)
  }
}
