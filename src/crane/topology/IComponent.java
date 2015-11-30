package crane.topology;

import java.io.Serializable;
import java.util.List;

public interface IComponent extends Serializable {
    public void addChild();

    public List<IComponent> getChildren();
    
    public int getParallelism();
}
