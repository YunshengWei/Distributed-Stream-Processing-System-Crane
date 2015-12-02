package crane.bolt;

import java.io.IOException;
import java.util.List;

import crane.task.OutputCollector;
import crane.topology.IComponent;
import crane.tuple.ITuple;

public interface IBolt extends IComponent {
    List<ITuple> map(ITuple tuple);

    default void execute(ITuple tuple, OutputCollector output) throws IOException {
        byte[] checksum = tuple.getSalt();
        List<ITuple> tuples = map(tuple);
        for (ITuple t : tuples) {
            output.emit(t, this, checksum);
        }
        output.ack(tuple.getID(), checksum);
    }
}
