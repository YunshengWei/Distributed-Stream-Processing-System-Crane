package crane.topology;

import java.util.List;

public interface IComponent {
    public void addBolt();

    public List<IComponent> getBolts();
}
