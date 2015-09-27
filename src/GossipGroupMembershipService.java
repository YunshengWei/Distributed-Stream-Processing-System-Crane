import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

public class GossipGroupMembershipService implements DaemonService {

    private class GossipSender implements Runnable {
        @Override
        public void run() {
            try {
                MembershipList mlToGossip = membershipList.updateAndGetNonFailMembers();
                GossipGroupMembershipService.gossipMembershipList(mlToGossip, sendSocket, 1);
            } catch (IOException e) {
                // Exception means voluntarily leaving,
                // so it's safe to ignore it.
            }
        }
    }

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
                    membershipList.merge(receivedMsl);
                }
            } catch (IOException e) {
                // Exception means voluntarily leaving,
                // so it's safe to ignore it.
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    private final MembershipList membershipList;
    private final DatagramSocket sendSocket;
    private final DatagramSocket recSocket;
    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> future;
    private long totalBandWidthUsage = 0;
    private long serviceStartTime;
    private boolean serving = false;

    private static void gossipMembershipList(MembershipList ml, DatagramSocket sendSocket,
            int count) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        synchronized (ml) {
            oos.writeObject(ml);
        }
        byte[] bytes = baos.toByteArray();
        oos.close();

        for (int i = 0; i < count; i++) {
            Address dest = ml.getRandomAliveMember();
            if (dest != null) {
                DatagramPacket packet = new DatagramPacket(bytes, bytes.length, dest.IP, dest.port);
                synchronized (sendSocket) {
                    sendSocket.send(packet);
                }
            }
        }

    }

    private GossipGroupMembershipService(DatagramSocket sendSocket, DatagramSocket recSocket,
            MembershipList membershipList, ScheduledExecutorService scheduler) {
        this.sendSocket = sendSocket;
        this.recSocket = recSocket;
        this.membershipList = membershipList;
        this.scheduler = scheduler;
    }

    public static GossipGroupMembershipService create(int portNumber, Address... introducers)
            throws SocketException, UnknownHostException {
        DatagramSocket recSocket = new DatagramSocket(portNumber);
        DatagramSocket sendSocket = new DatagramSocket();
        MembershipList membershipList = MembershipList.createMembershipList(
                new Address(InetAddress.getLocalHost(), portNumber), introducers);
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        return new GossipGroupMembershipService(sendSocket, recSocket, membershipList, scheduler);
    }

    // doc should mention return immediately, run in other threads.
    @Override
    public void startServe() {
        serving = true;
        serviceStartTime = System.currentTimeMillis();
        future = scheduler.scheduleAtFixedRate(new GossipSender(), 0, Catalog.GOSSIP_PERIOD,
                Catalog.GOSSIP_PERIOD_TIME_UNIT);
        new Thread(new GossipReceiver()).start();
    }

    @Override
    public void stopServe() {
        future.cancel(false);
        scheduler.shutdown();
        MembershipList vlm = membershipList.voluntaryLeaveMessage();
        try {
            GossipGroupMembershipService.gossipMembershipList(vlm, sendSocket,
                    Catalog.NUM_LEAVE_GOSSIP);
        } catch (IOException e) {
            // TODO maybe suppress exception?
            e.printStackTrace();
        }
        sendSocket.close();
        recSocket.close();
    }

    public List<Address> getMembers() {
        return membershipList.getAliveMembersExceptSelf();
    }

    public long getTotalBandwidthUsage() {
        return this.totalBandWidthUsage;
    }
    
    public long getServiceRunningTime() {
        
    }
}
