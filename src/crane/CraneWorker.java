package crane;

import crane.topology.IComponent;

/**
 * CraneWorker is worker thread assigned by Supervisor.
 */
public interface CraneWorker extends Runnable {
    /**
     * set the component for the worker.
     * 
     * @param comp
     */
    void setComponent(IComponent comp);
}
