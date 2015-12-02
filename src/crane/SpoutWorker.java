package crane;

import java.io.IOException;
import java.net.SocketException;
import java.util.logging.Level;
import java.util.logging.Logger;

import crane.spout.ISpout;
import crane.task.OutputCollector;
import crane.topology.Address;
import crane.topology.IComponent;
import crane.tuple.ITuple;

public class SpoutWorker implements CraneWorker {

    private ISpout spout;
    private final Logger logger;
    private final OutputCollector output;
    private final INimbus nimbus;

    public SpoutWorker(ISpout spout, Address ackerAddress, INimbus nimbus, Logger logger) throws SocketException {
        this.spout = spout;
        this.output = new OutputCollector(ackerAddress);
        this.nimbus = nimbus;
        this.logger = logger;
    }

    @Override
    public void run() {
        spout.open();
        ITuple tuple;
        try {
            while ((tuple = spout.nextTuple()) != null) {
                spout.execute(tuple, output);
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            spout.close();
        }
    }

    @Override
    public void setComponent(IComponent comp) {
        spout = (ISpout) comp;
    }
}
