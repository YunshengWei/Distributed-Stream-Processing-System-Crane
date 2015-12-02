package crane.task;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.SocketException;

import crane.AckMessage;
import crane.topology.Address;
import crane.topology.IComponent;
import crane.tuple.ITuple;
import system.CommonUtils;

public class OutputCollector {
    private final DatagramSocket sendSocket;
    private final Address ackerAddress;

    public OutputCollector(Address ackerAddress) throws SocketException {
        sendSocket = new DatagramSocket();
        this.ackerAddress = ackerAddress;
    }

    public void ack(int tupleID, byte[] checksum) throws IOException {
        AckMessage msg = new AckMessage(tupleID, checksum);
        CommonUtils.sendObjectOverUDP(msg, ackerAddress.IP, ackerAddress.port, sendSocket);
    }

    public void emit(ITuple tuple, IComponent comp, byte[] checksum) throws IOException {
        for (IComponent child : comp.getChildren()) {
            int taskNo = child.getPartitionStrategy().partition(tuple, child.getParallelism());
            tuple.setSalt();
            
            Address add = child.getTaskAddress(taskNo);
            CommonUtils.sendObjectOverUDP(tuple, add.IP, add.port, sendSocket);
            for (int i = 0; i < checksum.length; i++) {
                checksum[i] ^= tuple.getSalt()[i];
            }
        }
    }
}
