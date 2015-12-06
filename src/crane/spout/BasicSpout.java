package crane.spout;

import java.io.IOException;

import crane.partition.IPartitionStrategy;
import crane.task.OutputCollector;
import crane.topology.BasicComponent;
import crane.tuple.ITuple;

public abstract class BasicSpout extends BasicComponent implements ISpout {

    public BasicSpout(String componentID, int parallelism, IPartitionStrategy ps) {
        super(componentID, parallelism, ps);
    }

    private static final long serialVersionUID = 1L;

    private int tupleID = 0;

    @Override
    public void execute(ITuple tuple, OutputCollector output) throws IOException {
        long checksum = 0;
        tuple.setID(tupleID++);
        checksum = output.emit(tuple, this, checksum);
        output.ack(tuple.getID(), checksum);
    }
}
