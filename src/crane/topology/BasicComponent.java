package crane.topology;

import java.util.ArrayList;
import java.util.List;

import crane.partition.IPartitionStrategy;

public abstract class BasicComponent implements IComponent {

    private static final long serialVersionUID = 1L;

    private final List<IComponent> children;
    private IComponent parent;
    private final String componentID;
    private final int parallelism;
    private final Address[] addresses;
    private final IPartitionStrategy ps;

    public BasicComponent(String componentID, int parallelism, IPartitionStrategy ps) {
        this.componentID = componentID;
        this.parallelism = parallelism;
        this.children = new ArrayList<>();
        this.addresses = new Address[this.parallelism];
        this.ps = ps;
        parent = null;
    }

    @Override
    public void addChild(IComponent comp) {
        children.add(comp);
    }

    @Override
    public List<IComponent> getChildren() {
        return children;
    }

    @Override
    public IComponent getParent() {
        return parent;
    }
    
    @Override
    public void setParent(IComponent comp) {
        parent = comp;
    }

    @Override
    public int getParallelism() {
        return parallelism;
    }

    @Override
    public IPartitionStrategy getPartitionStrategy() {
        return this.ps;
    }

    @Override
    public void assign(int taskNo, Address address) {
        addresses[taskNo] = address;
    }

    @Override
    public String getComponentID() {
        return this.componentID;
    }

    @Override
    public Address getTaskAddress(int taskNo) {
        return addresses[taskNo];
    }

    /*
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof IComponent)) {
            return false;
        } else {
            IComponent that = (IComponent) o;
            return this.getComponentID().equals(that.getComponentID());
        }
    }

    @Override
    public int hashCode() {
        return getComponentID().hashCode();
    }*/
}
