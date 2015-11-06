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

public class LeaderElectionService extends Observable implements DaemonService, Observer {

    private GossipGroupMembershipService ggms;
    private Identity leader;
    private final Logger logger;

    public LeaderElectionService(GossipGroupMembershipService ggms, Logger logger) {
        this.ggms = ggms;
        this.logger = logger;
        this.leader = null;
    }

    private void setLeader(Identity leader) {
        synchronized (leader) {
            logger.info(String.format("Elect %s as the new leader.", leader));
            this.leader = leader;
            setChanged();
            notifyObservers(leader);
        }
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
        synchronized (ggms.getMembershipList()) {
            ggms.deleteObserver(this);
        }
    }

    public Identity getLeader() {
        synchronized (leader) {
            return leader;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void update(Observable o, Object arg) {
        List<Identity> failedNodes = ((ArrayList<ArrayList<Identity>>) arg).get(0);
        List<Identity> joiningNodes = ((ArrayList<ArrayList<Identity>>) arg).get(1);
        if (!joiningNodes.isEmpty()) {
            Identity oldestJoiningNode = Collections.min(joiningNodes, new Comparator<Identity>() {
                @Override
                public int compare(Identity o1, Identity o2) {
                    long t = o1.timestamp - o2.timestamp;
                    return t < 0 ? -1 : t == 0 ? 0 : 1;
                }
            });
            if (oldestJoiningNode.timestamp < leader.timestamp) {
                logger.info(String.format("Old leader %s is not the eldest.", leader));
                setLeader(oldestJoiningNode);
            }
        } else {
            if (failedNodes.contains(leader)) {
                Identity oldestMember = ggms.getOldestAliveMember();
                logger.info(String.format("Old leader %s failed.", leader));
                setLeader(oldestMember);
            }
        }
    }
}
