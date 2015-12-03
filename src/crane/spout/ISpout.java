package crane.spout;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.util.logging.Logger;

import crane.task.OutputCollector;
import crane.topology.Address;
import crane.topology.IComponent;
import crane.tuple.ITuple;

public interface ISpout extends IComponent {
    void open(Logger logger) throws IOException, NotBoundException;

    void close() throws IOException;

    ITuple nextTuple() throws IOException;

    void ack(int msgID);

    default void execute(ITuple tuple, OutputCollector output) throws IOException {
        long checksum = 0;
        checksum = output.emit(tuple, this, checksum);
        output.ack(tuple.getID(), checksum);
    }

    default Address getAddress() {
        return getTaskAddress(0);
    }
}
