package crane;

import java.io.IOException;
import java.net.InetAddress;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.logging.Logger;

import crane.topology.Address;
import crane.topology.IComponent;
import crane.topology.Topology;
import membershipservice.GossipGroupMembershipService;
import system.Catalog;
import system.CommonUtils;
import system.Identity;

public class Nimbus implements INimbus, Observer {

    private Registry registry;
    private Topology topology;
    private final GossipGroupMembershipService ggms;
    private Map<InetAddress, List<Task>> taskTracker;
    private Map<InetAddress, ISupervisor> supervisors;
    private Map<InetAddress, Integer> availablePorts;

    /**
     * the mutex is used to ensure only one job is executed at one time
     */
    private Semaphore mutex;

    private final Logger logger = CommonUtils.initializeLogger(Nimbus.class.getName(),
            Catalog.LOG_DIR + Catalog.NIMBUS_LOG, true);

    public Nimbus() throws IOException {
        mutex = new Semaphore(1);

        ggms = new GossipGroupMembershipService(InetAddress.getLocalHost(),
                Catalog.CRANE_MEMBERSHIP_SERVICE_PORT, Catalog.CRANE_MEMBERSHIP_SERVICE_PORT);
        ggms.addObserver(this);
        taskTracker = new HashMap<>();
        supervisors = new ConcurrentHashMap<>();
        availablePorts = new ConcurrentHashMap<>();

        ggms.startServe();

        INimbus stub = (INimbus) UnicastRemoteObject.exportObject(this, 0);
        registry = LocateRegistry.createRegistry(Catalog.NIMBUS_PORT);
        registry.rebind("nimbus", stub);
    }

    @Override
    public void submitTopology(Topology topology) throws RemoteException, IOException, InterruptedException {
        mutex.acquire();
        this.topology = topology;

        List<Task> tasks = new ArrayList<>();
        for (IComponent comp : topology) {
            for (int i = 0; i < comp.getParallelism(); i++) {
                tasks.add(new Task(comp, i));
            }
        }
        assignTasks(tasks);

        for (Task task : tasks) {
            ISupervisor sv = supervisors.get(task.comp.getTaskAddress(task.no).IP);
            sv.assignTask(task);
        }
    }

    private void assignTasks(List<Task> tasks) {
        int remainTasks = tasks.size();
        int k = 0;
        int i = 0;

        for (InetAddress ip : supervisors.keySet()) {
            int numTask = (int) Math.ceil(remainTasks / (double) (supervisors.size() - i++));
            int t = availablePorts.get(ip);
            availablePorts.put(ip, t + numTask);

            for (int j = 0; j < numTask; j++) {
                Task task = tasks.get(k++);
                task.comp.assign(task.no, new Address(ip, t + j));
                taskTracker.get(ip).add(task);
            }
            remainTasks -= numTask;
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
            taskTracker.remove(ip);
            availablePorts.remove(ip);
        }

        assignTasks(failedTasks);
    }

    @Override
    public void registerSupervisor(InetAddress ip, ISupervisor supervisor) throws RemoteException {
        supervisors.put(ip, supervisor);
        taskTracker.put(ip, new ArrayList<>());
        availablePorts.put(ip, Catalog.WORKER_PORT_RANGE);
    }

    @Override
    public void finishJob() {
        for (ISupervisor sv : supervisors.values()) {
            sv.terminateTasks();
        }
        this.topology = null;
        mutex.release();
    }

    public static void main(String[] args) throws IOException {
        new Nimbus();
    }
}
