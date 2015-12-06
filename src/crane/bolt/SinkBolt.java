package crane.bolt;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.List;

import crane.partition.RandomPartitionStrategy;
import crane.task.OutputCollector;
import crane.tuple.ITuple;
import crane.tuple.OneStringTuple;

public class SinkBolt extends BasicBolt {

    private static final long serialVersionUID = 1L;
    private final String outputFile;
    private transient PrintWriter pw;

    public SinkBolt(String componentID, String outputFile) {
        super(componentID, 1, new RandomPartitionStrategy());
        this.outputFile = outputFile;
    }

    @Override
    public List<ITuple> map(ITuple tuple) throws IOException {
        return Collections.singletonList(tuple);
    }

    @Override
    public void execute(ITuple tuple, OutputCollector output) throws IOException, InterruptedException {
        super.execute(tuple, output);
        if (pw == null) {
            pw = new PrintWriter(outputFile);
        }
        pw.println((String) ((OneStringTuple) tuple).getContent()[0]);
    }

    public void close() {
        pw.close();
    }
}
