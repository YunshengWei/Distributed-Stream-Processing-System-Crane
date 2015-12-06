package crane.bolt;

import java.util.Collections;
import java.util.List;

import crane.partition.IPartitionStrategy;
import crane.tuple.ITuple;
import crane.tuple.OneStringTuple;

public class OngoingEventFilterBolt extends BasicBolt {

    private static final long serialVersionUID = 1L;

    public OngoingEventFilterBolt(String componentID, int parallelism, IPartitionStrategy ps) {
        super(componentID, parallelism, ps);
    }

    @Override
    public List<ITuple> map(ITuple tuple) {
        OneStringTuple t = (OneStringTuple) tuple;
        String line = (String) t.getContent()[0];
        String[] fields = line.split(";");
        if (fields[3].equals("ongoing-event")) {
            return Collections.singletonList(new OneStringTuple(tuple.getID(), fields[0]));
        } else {
            return Collections.emptyList();
        }
    }

}
