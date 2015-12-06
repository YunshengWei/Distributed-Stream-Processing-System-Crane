package crane.bolt;

import java.io.IOException;
import java.util.List;

import crane.partition.IPartitionStrategy;
import crane.task.OutputCollector;
import crane.topology.BasicComponent;
import crane.tuple.ITuple;

public abstract class BasicBolt extends BasicComponent implements IBolt {

    public BasicBolt(String componentID, int parallelism, IPartitionStrategy ps) {
        super(componentID, parallelism, ps);
    }

    private static final long serialVersionUID = 1L;

    @Override
    public void execute(ITuple tuple, OutputCollector output) throws IOException {
        long checksum = tuple.getSalt();
        List<ITuple> tuples = map(tuple);
        for (ITuple t : tuples) {
            checksum = output.emit(t, this, checksum);
        }
        output.ack(tuple.getID(), checksum);
    }
}
