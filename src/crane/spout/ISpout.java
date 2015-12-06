package crane.spout;

import java.io.FileNotFoundException;
import java.io.IOException;

import crane.task.OutputCollector;
import crane.topology.Address;
import crane.topology.IComponent;
import crane.tuple.ITuple;

public interface ISpout extends IComponent {
    void open() throws FileNotFoundException;

    void close() throws IOException;

    ITuple nextTuple() throws IOException;

    void execute(ITuple tuple, OutputCollector output) throws IOException;

    default Address getAddress() {
        return getTaskAddress(0);
    }
}
