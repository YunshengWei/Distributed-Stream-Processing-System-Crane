package crane.bolt;

import java.util.ArrayList;
import java.util.List;

import crane.partition.IPartitionStrategy;
import crane.topology.BasicComponent;
import crane.tuple.ITuple;
import crane.tuple.OneStringTuple;

public class SplitSentenceBolt extends BasicComponent implements IBolt {

    private static final long serialVersionUID = 1L;
    
    public SplitSentenceBolt(String componentID, int parallelism, IPartitionStrategy ps) {
        super(componentID, parallelism, ps);
    }

    @Override
    public List<ITuple> map(ITuple tuple) {
        String line = (String) tuple.getContent()[0];
        String[] words = line.split("\\s+");
        
        List<ITuple> tuples = new ArrayList<>();
        int tupleId = tuple.getID();
        for (String word : words) {
            tuples.add(new OneStringTuple(tupleId, word));
        }
        
        return tuples;
    }

}
