package crane.task;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.logging.Logger;

import crane.topology.Address;
import crane.tuple.ITuple;
import system.Catalog;

public class BoltWorker implements CraneWorker {

    private Task task;
    private final DatagramSocket socket;
    private final Logger logger;
    private final OutputCollector output;

    public BoltWorker(Task task, int port, Address ackerAddress, Logger logger)
            throws SocketException {
        this.task = task;
        this.socket = new DatagramSocket(port);
        this.output = new OutputCollector(ackerAddress, socket);
        this.logger = logger;
    }

    @Override
    public void setTask(Task task) {
        this.task = task;
    }

    @Override
    public void run() {
        DatagramPacket packet = new DatagramPacket(new byte[Catalog.MAX_UDP_PACKET_BYTES],
                Catalog.MAX_UDP_PACKET_BYTES);
        try {
            while (true) {
                socket.receive(packet);
                ByteArrayInputStream bais = new ByteArrayInputStream(packet.getData());
                ITuple tuple = (ITuple) new ObjectInputStream(bais).readObject();
                task.comp.execute(tuple, output);
            }
        } catch (IOException | ClassNotFoundException e) {
            logger.info(task.getTaskId() + ": terminated.");
        }
    }
    
    @Override
    public void terminate() {
        this.socket.close();
    }
}
