package crane.bolt;

import java.io.Serializable;

import crane.topology.IComponent;
import crane.tuple.Tuple;

public interface IBolt extends IComponent {
    public void execute(Tuple tuple);
}
