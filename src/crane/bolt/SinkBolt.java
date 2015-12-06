package crane.bolt;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import crane.partition.RandomPartitionStrategy;
import crane.task.OutputCollector;
import crane.tuple.ITuple;
import crane.tuple.OneStringTuple;

public class SinkBolt extends BasicBolt {

    private static final long serialVersionUID = 1L;
    private final String outputFile;

    public SinkBolt(String componentID, String outputFile) {
        super(componentID, 1, new RandomPartitionStrategy());
        this.outputFile = outputFile;
    }

    @Override
    public List<ITuple> map(ITuple tuple) throws IOException {
        return Collections.singletonList(tuple);
    }

    @Override
    public void execute(ITuple tuple, OutputCollector output) throws IOException {
        super.execute(tuple, output);
        System.out.println((String) ((OneStringTuple) tuple).getContent()[0]);
    }
}
