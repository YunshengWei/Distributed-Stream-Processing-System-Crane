package sdfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Observable;
import java.util.Observer;
import java.util.logging.Logger;

import membershipservice.GossipGroupMembershipService;
import system.Catalog;
import system.DaemonService;
import system.Identity;

/**
 * LeaderElectionService is built on top of GossipGroupMembershipService. When
 * Starting a LeaderElectionService, the underlying GossipGroupMembershipService
 * must already have started.
 */
public class LeaderElectionService extends Observable implements DaemonService, Observer {

    private final GossipGroupMembershipService ggms;
    private Identity leader;
    private final Logger logger;

    public LeaderElectionService(GossipGroupMembershipService ggms, Logger logger) {
        this.ggms = ggms;
        this.logger = logger;
        this.leader = null;
    }

    // This method needs to be synchronized. Very trick!
    private synchronized void setLeader(Identity leader) {
        logger.info(String.format("Elect %s as the new leader.", leader));
        this.leader = leader;
        setChanged();
        notifyObservers(leader);
    }

    // This method needs to be synchronized.
    public synchronized Identity getLeader() {
        return leader;
    }

    // This method needs to be synchronized.
    @Override
    public synchronized void addObserver(Observer o) {
        if (this.leader != null) {
            o.update(this, this.leader);
        }
        super.addObserver(o);
    }

    @Override
    public void startServe() throws IOException {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(Catalog.MEMBER_JOIN_TIME);
                    // Very tricky here!
                    // When setting the leader, we need to ensure membership
                    // list of group membership service does not change.
                    synchronized (ggms.getMembershipList()) {
                        setLeader(ggms.getOldestAliveMember());
                        ggms.addObserver(LeaderElectionService.this);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    @Override
    public void stopServe() {
        // Observable class in Java already takes care of thread safety
        // The worst race condition is that when the service has stopped, it is
        // notified one more time, but it's fine.
        ggms.deleteObserver(this);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void update(Observable o, Object arg) {

        // List<Identity> failedNodes = ((ArrayList<ArrayList<Identity>>)
        // arg).get(0);
        // List<Identity> joiningNodes = ((ArrayList<ArrayList<Identity>>)
        // arg).get(1);
        // if (!joiningNodes.isEmpty()) {
        // Identity oldestJoiningNode = Collections.min(joiningNodes, new
        // Comparator<Identity>() {
        // @Override
        // public int compare(Identity o1, Identity o2) {
        // long t = o1.timestamp - o2.timestamp;
        // return t < 0 ? -1 : t == 0 ? 0 : 1;
        // }
        // });
        // if (oldestJoiningNode.timestamp < leader.timestamp) {
        // logger.info(String.format("Old leader %s is not the eldest.",
        // leader));
        // setLeader(oldestJoiningNode);
        // }
        // } else {
        // if (failedNodes.contains(leader)) {
        // Identity oldestMember = ggms.getOldestAliveMember();
        // logger.info(String.format("Old leader %s failed.", leader));
        // setLeader(oldestMember);
        // }
        // }

        // For simplicity, assume we will always elect the eldest member as the
        // leader, i.e., it will not be detected
        // as failure when at first election.
        List<Identity> failedNodes = ((ArrayList<ArrayList<Identity>>) arg).get(0);
        if (failedNodes.contains(leader)) {
            Identity oldestMember = ggms.getOldestAliveMember();
            logger.info(String.format("Old leader %s failed.", leader));
            setLeader(oldestMember);
        }
    }
}
