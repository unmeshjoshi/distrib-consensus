package com.dist.consensus;

import org.junit.Test;

import static org.junit.Assert.*;

class StubClock extends SystemClock {
    private long fixedTime;

    public StubClock(long fixedTime) {
        this.fixedTime = fixedTime;
    }

    @Override
    public long nanoTime() {
        return fixedTime;
    }

    public void tickBy(long time) {
        fixedTime = fixedTime + time;
    }
}
public class ClusterClockTest {

    @Test
    public void shouldSetClusterTimeEpochFromLastLogEntry() {
        StubClock stubClock = new StubClock(10000);
        ClusterClock clusterClock = new ClusterClock(stubClock);
        long clusterTime = 1000L;
        clusterClock.newEpoch(clusterTime);
        assertEquals(clusterTime, clusterClock.clusterTimeAtEpoch);
        assertEquals(stubClock.nanoTime(), clusterClock.localTimeAtEpoch);
    }

    @Test
    public void shouldGenerateNewLeaderStamp() {
        StubClock stubClock = new StubClock(10000);
        ClusterClock clusterClock = new ClusterClock(stubClock);
        long clusterTime = 1000L;
        clusterClock.newEpoch(clusterTime);

        stubClock.tickBy(100);
        assertEquals(1100, clusterClock.leaderStamp());
        assertEquals(10100, clusterClock.localTimeAtEpoch);
    }

    @Test
    public void shouldInterpolateWithoutChangingClusterClockState() {
        StubClock stubClock = new StubClock(10000);
        ClusterClock clusterClock = new ClusterClock(stubClock);
        long clusterTime = 1000L;
        clusterClock.newEpoch(clusterTime);

        stubClock.tickBy(100);
        assertEquals(1100, clusterClock.interpolate());
        assertEquals(10000, clusterClock.localTimeAtEpoch);
    }
}