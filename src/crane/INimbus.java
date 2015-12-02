package crane;

import java.io.IOException;
import java.net.InetAddress;
import java.rmi.Remote;
import java.rmi.RemoteException;

import crane.topology.Topology;

/**
 * Nimbus is the master node for Crane. Nimbus is responsible for receiving job
 * from clients, assigning tasks to supervisors, and monitoring for failures. We
 * assume Nimbus tracks at most one job at the same time, and nimbus never dies.
 * (This is a reasonable assumption.)
 */
public interface INimbus extends Remote {
    /**
     * Submit the job to nimbus.
     * 
     * @param topology
     *            the topology for the job
     * @throws RemoteException
     * @throws IOException
     */
    void submitTopology(Topology topology) throws RemoteException, IOException;

    /**
     * Supervisor join the system by calling the method.
     * 
     * @param selfIP
     *            the IP address of the caller supervisor
     * @param supervisor
     *            the Remote stub for the caller supervisor
     * @throws RemoteException
     */
    void registerSupervisor(InetAddress selfIP, ISupervisor supervisor) throws RemoteException;

    /**
     * Query the current status.
     * 
     * @return a brief summary of the current status
     * @throws RemoteException
     */
    String queryStatus() throws RemoteException;
}
