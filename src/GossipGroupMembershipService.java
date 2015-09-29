import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

/**
 * GossipGroupMembershipService is a daemon service, which implements gossip
 * membership protocol.
 */
public class GossipGroupMembershipService implements DaemonService {

    /**
     * GossipSender takes responsibility for gossiping its membership list to
     * other alive members.
     */
    private class GossipSender implements Runnable {
        @Override
        public void run() {
            try {
                List<MembershipList.MemberStateChange> mscList = membershipList.update();
                for (MembershipList.MemberStateChange msc : mscList) {
                    System.out.println(msc);
                }
                if (mscList.size() > 0) {
                    System.out.println(membershipList);
                }
                Identity id = membershipList.getRandomAliveMember();
                if (id != null) {
                    sendMembershipList(membershipList.getNonFailMembers(), id.IPAddress);
                }
            } catch (IOException e) {
                // Exception means voluntarily leaving,
                // so it's safe to ignore it.
            }
        }
    }

    /**
     * IntroducerNegotiator is responsible for sending membership list to
     * introducer. Directly sending membership list to introducer is essential
     * for allowing introducer to function normally after restoring from crash.
     */
    private class IntroducerNegotiator implements Runnable {
        @Override
        public void run() {
            try {
                // It is ok to not update membership list here
                sendMembershipList(membershipList.getNonFailMembers(), introducerIP);
            } catch (IOException e) {
                // Exception means voluntarily leaving,
                // so it's safe to ignore it.
            }
        }
    }

    /**
     * GossipReceiver takes responsibility for receiving membership lists
     * gossiped from other members, and merging them with self local membership
     * list.
     */
    private class GossipReceiver implements Runnable {
        @Override
        public void run() {
            DatagramPacket packet = new DatagramPacket(new byte[Catalog.MAX_UDP_PACKET_BYTES],
                    Catalog.MAX_UDP_PACKET_BYTES);
            try {
                while (true) {
                    recSocket.receive(packet);
                    ByteArrayInputStream bais = new ByteArrayInputStream(packet.getData());
                    MembershipList receivedMsl = (MembershipList) new ObjectInputStream(bais)
                            .readObject();
                    List<MembershipList.MemberStateChange> mscList = membershipList
                            .merge(receivedMsl);
                    for (MembershipList.MemberStateChange msc : mscList) {
                        System.out.println(msc);
                    }
                    if (mscList.size() > 0) {
                        System.out.println(membershipList);
                    }
                }
            } catch (IOException e) {
                // Exception means voluntarily leaving,
                // so it's safe to ignore it.
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Send a specified membership list to target over UDP.
     * 
     * @param ml
     *            the membership list to send
     * @param target
     *            the IP address of the receiver
     * @throws IOException
     *             if any IO error occurs
     */
    private void sendMembershipList(MembershipList ml, InetAddress target) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        synchronized (ml) {
            oos.writeObject(ml);
        }
        byte[] bytes = baos.toByteArray();
        oos.close();

        DatagramPacket packet = new DatagramPacket(bytes, bytes.length, target,
                Catalog.MEMBERSHIP_SERVICE_PORT);
        synchronized (sendSocket) {
            sendSocket.send(packet);
        }

    }

    private final InetAddress introducerIP;
    private MembershipList membershipList;
    private DatagramSocket sendSocket;
    private DatagramSocket recSocket;
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> future1, future2;

    public GossipGroupMembershipService(InetAddress introducerIP) {
        this.introducerIP = introducerIP;
    }

    @Override
    public void startServe() throws IOException {
        recSocket = new DatagramSocket(Catalog.MEMBERSHIP_SERVICE_PORT);
        sendSocket = new DatagramSocket();
        membershipList = new MembershipList(
                new Identity(InetAddress.getLocalHost(), System.currentTimeMillis()));
        scheduler = Executors.newScheduledThreadPool(2);

        future1 = scheduler.scheduleAtFixedRate(new GossipSender(), 0, Catalog.GOSSIP_PERIOD,
                Catalog.GOSSIP_PERIOD_TIME_UNIT);
        if (!introducerIP.equals(InetAddress.getLocalHost())) {
            future2 = scheduler.scheduleAtFixedRate(new IntroducerNegotiator(), 0,
                Catalog.INTRODUCER_NEGOTIATE_PERIOD, Catalog.GOSSIP_PERIOD_TIME_UNIT);
        } else {
            future2 = null;
        }
        new Thread(new GossipReceiver()).start();
    }

    @Override
    public void stopServe() {
        future1.cancel(false);
        if (future2 != null) {
            future2.cancel(false);
        }
        scheduler.shutdown();
        recSocket.close();

        MembershipList vlm = membershipList.voluntaryLeaveMessage();
        try {
            for (int i = 0; i < Catalog.NUM_LEAVE_GOSSIP; i++) {
                Identity id = membershipList.getRandomAliveMember();
                if (id != null) {
                    sendMembershipList(vlm, id.IPAddress);
                }
            }
        } catch (IOException e) {
            // IOExceptin here means real IOException
            e.printStackTrace();
        }
        sendSocket.close();
    }

    /**
     * @return get a list of alive members in the group including self.
     */
    public List<Identity> getAliveMembers() {
        return membershipList.getAliveMembersIncludingSelf();
    }
}
