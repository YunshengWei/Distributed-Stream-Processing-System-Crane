package crane;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import crane.task.AckMessage;
import crane.task.CraneWorker;
import crane.task.Task;
import crane.topology.Address;
import system.Catalog;
import system.CommonUtils;

public class Acker implements CraneWorker {

    // Assume spout never dies, otherwise there is no way to tell apart ack
    // message from previous round and current round
    private Address spoutAddress;
    private final DatagramSocket ds;
    private final Logger logger;
    private final Map<Integer, Long> tupleChecksums;

    public Acker(Address spoutAddress, int port, Logger logger) throws SocketException {
        this.spoutAddress = spoutAddress;
        this.ds = new DatagramSocket(port);
        this.logger = logger;
        this.tupleChecksums = Collections.synchronizedMap(new HashMap<>());
    }

    public void setSpoutAddress(Address spoutAddress) {
        this.spoutAddress = spoutAddress;
        tupleChecksums.clear();
    }

    @Override
    public void run() {
        DatagramPacket packet = new DatagramPacket(new byte[Catalog.MAX_UDP_PACKET_BYTES],
                Catalog.MAX_UDP_PACKET_BYTES);
        try {
            while (true) {
                ds.receive(packet);
                ByteArrayInputStream bais = new ByteArrayInputStream(packet.getData());
                AckMessage msg = (AckMessage) new ObjectInputStream(bais).readObject();
                
                int tid = msg.tupleID;
                long checksum = msg.checksum;
                synchronized (tupleChecksums) {
                    long cs = tupleChecksums.getOrDefault(tid, 0L);
                    cs ^= checksum;
                    if (cs == 0) {
                        tupleChecksums.remove(tid);
                        CommonUtils.sendObjectOverUDP(tid, spoutAddress.IP, spoutAddress.port, ds);
                    } else {
                        tupleChecksums.put(tid, cs);
                    }
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            logger.info("Acker terminated.");
        }
    }

    @Override
    public void setTask(Task task) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void terminate() {
        ds.close();
    }

}
