package crane;

import java.rmi.Remote;
import java.rmi.RemoteException;

import crane.topology.IComponent;

public interface ISupervisor extends Remote {
    void assignComponent(IComponent component) throws RemoteException;
}
