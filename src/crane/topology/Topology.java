package crane.topology;

import java.io.Serializable;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;

import crane.spout.ISpout;

public class Topology implements Iterable<IComponent>, Serializable {

    private static final long serialVersionUID = 1L;
    private ISpout spout = null;

    public int size() {
        int size = 0;
        for (IComponent comp : this) {
            size += comp.getParallelism();
        }
        return size;
    }

    public void setSpout(ISpout spout) {
        this.spout = spout;
    }

    public ISpout getSpout() {
        return this.spout;
    }

    private class TopologyIterator implements Iterator<IComponent> {
        private final Deque<IComponent> stack;

        TopologyIterator() {
            stack = new LinkedList<>();
            if (spout != null) {
                stack.addLast(spout);
            }
        }

        @Override
        public boolean hasNext() {
            return !stack.isEmpty();
        }

        @Override
        public IComponent next() {
            IComponent comp = stack.removeLast();
            for (IComponent child : comp.getChildren()) {
                stack.addLast(child);
            }
            return comp;
        }
    }

    @Override
    public Iterator<IComponent> iterator() {
        return new TopologyIterator();
    }

}
