package crane;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.logging.Level;
import java.util.logging.Logger;

import crane.bolt.IBolt;
import crane.task.OutputCollector;
import crane.topology.Address;
import crane.topology.IComponent;
import crane.tuple.ITuple;
import system.Catalog;

public class BoltWorker implements CraneWorker {

    private IBolt bolt;
    private final DatagramSocket recSocket;
    private final Logger logger;
    private final OutputCollector output;

    public BoltWorker(IBolt bolt, DatagramSocket recSocket, Address ackerAddress, Logger logger)
            throws SocketException {
        this.bolt = bolt;
        this.recSocket = recSocket;
        this.output = new OutputCollector(ackerAddress);
        this.logger = logger;
    }

    @Override
    public void setComponent(IComponent comp) {
        this.bolt = (IBolt) comp;
    }
    
    @Override
    public void run() {
        DatagramPacket packet = new DatagramPacket(new byte[Catalog.MAX_UDP_PACKET_BYTES],
                Catalog.MAX_UDP_PACKET_BYTES);
        try {
            while (true) {
                recSocket.receive(packet);
                ByteArrayInputStream bais = new ByteArrayInputStream(packet.getData());
                ITuple tuple = (ITuple) new ObjectInputStream(bais).readObject();
                bolt.execute(tuple, output);
            }
        } catch (IOException | ClassNotFoundException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
    }
}
