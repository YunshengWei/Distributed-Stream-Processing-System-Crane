package crane;

import java.io.IOException;
import java.net.SocketException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * Supervisor listens for work assigned to its machine, and starts worker
 * threads based on what Nimbus has assigned to it.
 */
public interface ISupervisor extends Remote {
    /**
     * assign the specified task to the machine.
     * 
     * @param task
     * @throws RemoteException
     * @throws SocketException
     */
    void assignTask(Task task) throws RemoteException, SocketException;

    /**
     * notify the supervisor that information about the given task needs to be
     * updated. It is Nimbus's responsibility to ensure the given task is
     * running on the machine.
     * 
     * @param task
     * @throws RemoteException
     */
    void updateTask(Task task) throws RemoteException;

    /**
     * Register ports on the machine.
     * 
     * @param numPorts
     *            the number of ports to register
     * @return the registered ports
     * @throws RemoteException
     * @throws IOException
     */
    List<Integer> registerPorts(int numPorts) throws RemoteException, IOException;
}
