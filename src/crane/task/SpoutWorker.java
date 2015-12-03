package crane.task;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.rmi.NotBoundException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import crane.INimbus;
import crane.spout.ISpout;
import crane.topology.Address;
import crane.tuple.ITuple;
import system.Catalog;

public class SpoutWorker implements CraneWorker {

    private class TupleStatus {
        ITuple tuple;
        long timestamp;
        
        TupleStatus(ITuple tuple, long timestamp) {
            this.tuple = tuple;
            this.timestamp = timestamp;
        }
    }

    private class AckReceiver implements CraneWorker {
        DatagramSocket ds;

        AckReceiver() throws SocketException {
            ds = new DatagramSocket(task.getTaskAddress().port);
        }

        @Override
        public void run() {
            DatagramPacket packet = new DatagramPacket(new byte[Catalog.MAX_UDP_PACKET_BYTES],
                    Catalog.MAX_UDP_PACKET_BYTES);
            try {
                while (true) {
                    ds.receive(packet);
                    ByteArrayInputStream bais = new ByteArrayInputStream(packet.getData());
                    int tupleId = (int) new ObjectInputStream(bais).readObject();
                    
                    completedTuple.add(tupleId);
                    pendingTuples.remove(tupleId);
                }
            } catch (IOException | ClassNotFoundException e) {
                logger.info("Ack receiver thread terminated.");
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

    private final Task task;
    private final Logger logger;
    private final OutputCollector output;
    private final INimbus nimbus;
    private final Set<Integer> completedTuple;
    private final Map<Integer, TupleStatus> pendingTuples;

    public SpoutWorker(Task task, Address ackerAddress, INimbus nimbus, Logger logger)
            throws SocketException {
        this.task = task;
        this.output = new OutputCollector(ackerAddress,
                new DatagramSocket(task.getTaskAddress().port));
        this.nimbus = nimbus;
        this.logger = logger;
        this.completedTuple = Collections.synchronizedSet(new HashSet<>());
        this.pendingTuples = Collections.synchronizedMap(new LinkedHashMap<>());
    }

    @Override
    public void setTask(Task task) {
        // can not simply reassign task, because task in Spout maintains
        // information about the position in file
        // this.task = task;

        // Because we are only changing references, it is OK to not use locks.
        for (int i = 0; i < task.comp.getChildren().size(); i++) {
            this.task.comp.getChildren().set(i, task.comp.getChildren().get(i));
        }
    }

    @Override
    public void run() {
         try {
             AckReceiver ar = new AckReceiver();
             new Thread(ar).start();
             
             ISpout spout = (ISpout) task.comp;
             spout.open(logger);
             
             ITuple tuple;
             while ((tuple = spout.nextTuple()) != null) {
                 spout.execute(tuple, output);
             }
             
             spout.close();
             
             if (pendingTuples.isEmpty()) {
                 nimbus.finishJob();
             }
         } catch (IOException | NotBoundException e) {
             logger.log(Level.SEVERE, e.getMessage(), e);
         }
    }

    @Override
    public void terminate() {
    }
}
