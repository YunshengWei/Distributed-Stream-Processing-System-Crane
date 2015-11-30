package crane;

import java.rmi.Remote;
import java.rmi.RemoteException;

import crane.topology.Topology;

public interface INimbus extends Remote {
    void submitTopology(Topology topology) throws RemoteException;
}
