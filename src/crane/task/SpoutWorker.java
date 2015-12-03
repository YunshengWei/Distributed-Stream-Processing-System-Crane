package crane.task;

import java.io.IOException;
import java.net.SocketException;
import java.rmi.NotBoundException;
import java.util.logging.Level;
import java.util.logging.Logger;

import crane.INimbus;
import crane.spout.ISpout;
import crane.topology.Address;
import crane.tuple.ITuple;

public class SpoutWorker implements CraneWorker {

    private Task task;
    private final Address ackerAddress;
    private final Logger logger;
    private final OutputCollector output;
    private final INimbus nimbus;

    public SpoutWorker(Task task, Address ackerAddress, INimbus nimbus, Logger logger)
            throws SocketException {
        this.ackerAddress = ackerAddress;
        this.task = task;
        this.output = new OutputCollector(ackerAddress);
        this.nimbus = nimbus;
        this.logger = logger;
    }

    @Override
    public void setTask(Task task) {
        this.task = task;
    }
    
    @Override
    public void run() {
        /*ISpout spout = (ISpout) task.comp;
        try {
            spout.open(logger);
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        } catch (NotBoundException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        ITuple tuple;
        try {
            while ((tuple = spout.nextTuple()) != null) {
                spout.execute(tuple, output);
            }
            nimbus.finishJob();
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            spout.close();
        }*/
    }
    
    @Override
    public void terminate() {
    }
}
