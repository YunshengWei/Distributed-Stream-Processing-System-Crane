package crane;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.util.List;

import crane.topology.Topology;
import membershipservice.GossipGroupMembershipService;
import system.Catalog;
import system.DaemonService;
import system.Identity;

public class Nimbus implements INimbus, DaemonService {

    private final GossipGroupMembershipService ggms;

    public Nimbus() throws UnknownHostException {
        ggms = new GossipGroupMembershipService(InetAddress.getLocalHost(),
                Catalog.CRANE_MEMBERSHIP_SERVICE_PORT, Catalog.CRANE_MEMBERSHIP_SERVICE_PORT);
    }

    @Override
    public void startServe() throws IOException {
    }

    @Override
    public void stopServe() throws Exception {
    }

    @Override
    public void submitTopology(Topology topology) throws RemoteException {
        int remain = topology.size();
        List<Identity> members = ggms.getMembershipList().getAliveMembersExceptSelf();

        int i = 0;
    }

}
