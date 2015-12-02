package crane.spout;

import java.io.IOException;

import crane.task.OutputCollector;
import crane.topology.IComponent;
import crane.tuple.ITuple;
import system.Catalog;

public interface ISpout extends IComponent {
    void open();

    void close();

    ITuple nextTuple();

    void ack(int msgID);

    void fail(int msgID);

    default void execute(ITuple tuple, OutputCollector output) throws IOException {
        byte[] checksum = new byte[Catalog.CHECKSUM_LENGTH];
        output.emit(tuple, this, checksum);
        output.ack(tuple.getID(), checksum);
    }
}
