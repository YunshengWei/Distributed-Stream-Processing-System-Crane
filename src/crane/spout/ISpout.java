package crane.spout;

import crane.topology.IComponent;

public interface ISpout extends IComponent {
    public void nextTuple();
}
