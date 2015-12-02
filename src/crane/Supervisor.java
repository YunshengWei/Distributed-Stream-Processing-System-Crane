package crane;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import crane.bolt.IBolt;
import crane.spout.ISpout;
import crane.topology.Address;
import membershipservice.GossipGroupMembershipService;
import sdfs.Datanode;
import system.Catalog;
import system.CommonUtils;

public class Supervisor implements ISupervisor {

    private final GossipGroupMembershipService ggms;
    private final Map<String, CraneWorker> taskTracker;
    private final Map<Integer, DatagramSocket> boundSockets;
    private final Logger logger = CommonUtils.initializeLogger(Supervisor.class.getName(),
            Catalog.LOG_DIR + Catalog.SUPERVISOR_LOG, true);
    private final Address ackerAddress;
    private final INimbus nimbus;

    public Supervisor() throws NotBoundException, IOException {
        // For simplicity, assume acker is on the same machine as Nimbus
        ackerAddress = new Address(InetAddress.getByName(Catalog.NIMBUS_ADDRESS),
                Catalog.ACKER_PORT);
        ggms = new GossipGroupMembershipService(InetAddress.getByName(Catalog.NIMBUS_ADDRESS),
                Catalog.CRANE_MEMBERSHIP_SERVICE_PORT, Catalog.CRANE_MEMBERSHIP_SERVICE_PORT);
        taskTracker = new ConcurrentHashMap<>();
        boundSockets = new ConcurrentHashMap<>();
        
        ggms.startServe();
        Registry registry = LocateRegistry.getRegistry(Catalog.NIMBUS_ADDRESS, Catalog.NIMBUS_PORT);
        nimbus = (INimbus) registry.lookup("nimbus");
        
        ISupervisor stub = (ISupervisor) UnicastRemoteObject.exportObject(this, Catalog.SUPERVISOR_PORT);
        nimbus.registerSupervisor(InetAddress.getLocalHost(), stub);
    }

    @Override
    public void assignTask(Task task) throws RemoteException, SocketException {
        CraneWorker worker;
        if (task.comp instanceof IBolt) {
            int port = task.comp.getTaskAddress(task.no).port;
            DatagramSocket ds = boundSockets.get(port);
            worker = new BoltWorker((IBolt) task.comp, ds, ackerAddress, logger);
        } else {
            worker = new SpoutWorker((ISpout) task.comp, ackerAddress, nimbus, logger);
        }

        taskTracker.put(task.getTaskId(), worker);
        new Thread(worker).start();
    }

    @Override
    public void updateTask(Task task) throws RemoteException {
        taskTracker.get(task.getTaskId()).setComponent(task.comp);
    }

    @Override
    public List<Integer> registerPorts(int numPorts) throws RemoteException, IOException {
        List<Integer> ports = new ArrayList<>();
        for (int i = 0; i < numPorts; i++) {
            DatagramSocket ds = new DatagramSocket();
            boundSockets.put(ds.getLocalPort(), ds);
            ports.add(ds.getLocalPort());
        }
        return ports;
    }

    public void main(String[] args) throws IOException, NotBoundException {
        new Supervisor();
    }
}
