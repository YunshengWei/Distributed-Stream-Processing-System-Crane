package crane;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import crane.topology.Address;
import crane.topology.IComponent;
import crane.topology.Topology;
import membershipservice.GossipGroupMembershipService;
import system.Catalog;
import system.DaemonService;
import system.Identity;

/**
 * Assume Nimbus tracks at most one job at the same time, and nimbus never dies.
 * (This is a reasonable assumption.)
 */
public class Nimbus implements INimbus, DaemonService, Observer {

    private Registry registry;
    private Topology topology;
    private final GossipGroupMembershipService ggms;
    private Map<InetAddress, Set<Task>> taskTracker;
    private Map<InetAddress, ISupervisor> supervisors;
    private Map<String, Boolean> jobStatus;
    private final Logger logger;

    public Nimbus() throws UnknownHostException {
        ggms = new GossipGroupMembershipService(InetAddress.getLocalHost(),
                Catalog.CRANE_MEMBERSHIP_SERVICE_PORT, Catalog.CRANE_MEMBERSHIP_SERVICE_PORT);
        ggms.addObserver(this);
        taskTracker = new HashMap<>();
        supervisors = new ConcurrentHashMap<>();
        jobStatus = new ConcurrentHashMap<>();
        logger = null;
    }

    @Override
    public void startServe() throws IOException {
        ggms.startServe();

        INimbus stub = (INimbus) UnicastRemoteObject.exportObject(this, 0);
        registry = LocateRegistry.createRegistry(Catalog.NIMBUS_PORT);
        registry.rebind("nimbus", stub);
    }

    @Override
    public void stopServe() throws Exception {
        ggms.stopServe();

        registry.unbind("nimbus");
        UnicastRemoteObject.unexportObject(registry, true);
        UnicastRemoteObject.unexportObject(this, true);

        supervisors = null;
    }

    @Override
    public void submitTopology(Topology topology) throws RemoteException, IOException {
        jobStatus.put(topology.topologyID, false);
        this.topology = topology;
        
        List<Task> tasks = new ArrayList<>();
        for (IComponent comp : topology) {
            for (int i = 0; i < comp.getParallelism(); i++) {
                tasks.add(new Task(comp, i));
            }
        }
        assignTasks(tasks);
        
        for (Task task : tasks) {
            ISupervisor sv = supervisors.getTaskAddress(task.comp.getSlot(task.no).IP);
            sv.assignTask(task);
        }
        
//        int remainTasks = topology.size();
//
//        List<Address> addresses = new ArrayList<>();
//        int i = 0;
//        for (Map.Entry<InetAddress, ISupervisor> entry : supervisors.entrySet()) {
//            int numTask = (int) Math.ceil(remainTasks / (double) (supervisors.size() - i));
//            i += 1;
//
//            ISupervisor supervisor = entry.getValue();
//            InetAddress ip = entry.getKey();
//            taskTracker.put(ip, new HashSet<>());
//            List<Integer> ports = supervisor.registerPorts(numTask);
//
//            remainTasks -= numTask;
//            for (int port : ports) {
//                addresses.add(new Address(ip, port));
//            }
//        }

//        i = 0;
//        for (IComponent comp : topology) {
//            for (int j = 0; j < comp.getParallelism(); j++) {
//                comp.assign(j, addresses.get(i++));
//            }
//        }
//
//        for (IComponent comp : topology) {
//            for (int j = 0; j < comp.getParallelism(); j++) {
//                InetAddress ip = comp.getSlot(j).IP;
//                int port = comp.getSlot(j).port;
//                ISupervisor supervisor = supervisors.get(ip);
//                supervisor.assignComponent(comp, port);
//                taskTracker.get(ip).add(comp);
//            }
//        }
    }

    private void assignTasks(List<Task> tasks) {
        int remainTasks = tasks.size();
        int k = 0;
        int i = 0;

        for (Map.Entry<InetAddress, ISupervisor> entry : supervisors.entrySet()) {
            int numTask = (int) Math.ceil(remainTasks / (double) (supervisors.size() - i));
            InetAddress ip = entry.getKey();
            ISupervisor supervisor = entry.getValue();
            try {
                List<Integer> availablePorts = supervisor.getAvailablePorts(numTask);

                for (int j = 0; j < numTask; j++) {
                    Task task = tasks.get(k++);
                    task.comp.assign(task.no, new Address(ip, availablePorts.get(j)));
                    taskTracker.get(ip).add(task);
                }
                remainTasks -= numTask;
            } catch (RemoteException e) {

            }
        }
    }

    @Override
    public void update(Observable o, Object arg) {
        List<Task> failedTasks = new ArrayList<>();

        @SuppressWarnings("unchecked")
        List<Identity> failedNodes = (ArrayList<Identity>) arg;
        for (Identity id : failedNodes) {
            InetAddress ip = id.IPAddress;
            supervisors.remove(ip);
            for (Task task : taskTracker.get(ip)) {
                failedTasks.add(task);
            }
        }

        assignTasks(failedTasks);
    }

    @Override
    public void registerSupervisor(InetAddress ip, ISupervisor supervisor) throws RemoteException {
        supervisors.put(ip, supervisor);
        taskTracker.put(ip, new HashSet<>());
    }

    public static void main(String[] args) {
    }

    @Override
    public void ack(int tupldID, String componentID) throws RemoteException {
        // TODO Auto-generated method stub

    }
}
