package sdfs;

import java.io.IOException;
import java.net.InetAddress;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import membershipservice.GossipGroupMembershipService;
import system.Catalog;
import system.DaemonService;
import system.Identity;

public class NamenodeService implements DaemonService, Namenode, Observer {

    private Namenode stub;
    private Registry registry;
    private Metadata metadata;
    private GossipGroupMembershipService ggms;
    private ScheduledExecutorService scheduler;
    private final Logger logger;

    public NamenodeService(GossipGroupMembershipService ggms, Logger logger) {
        this.ggms = ggms;
        this.logger = logger;
    }

    @Override
    public List<Datanode> getFileLocations(String fileName) throws RemoteException {
        return metadata.getFileLocations(fileName);
    }

    @Override
    public List<InetAddress> getFileLocationIPs(String fileName) throws RemoteException {
        return metadata.getFileLocationIPs(fileName);
    }

    @Override
    public List<Datanode> putRequest() throws RemoteException {
        return metadata.getKidlestNodes(Catalog.REPLICATION_FACTOR);
    }

    @Override
    public void deleteFile(String fileName) throws RemoteException, IOException {
        List<Datanode> locations = metadata.getFileLocations(fileName);
        for (Datanode datanode : locations) {
            datanode.deleteFile(fileName);
        }
        metadata.deleteFile(fileName);
    }

    @Override
    public void updateMetadate(BlockReport blockReport) throws RemoteException {
        metadata.mergeBlockReport(blockReport);
    }

    private class CheckReplication implements Runnable {
        @Override
        public void run() {
            try {
                List<Object> request = metadata.getReplicationRequest();
                if (request != null) {
                    String file = (String) request.get(0);
                    Datanode from = (Datanode) request.get(1);
                    Datanode[] tos = request.subList(2, request.size()).toArray(new Datanode[0]);
                    from.replicateFile(file, tos);
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, e.getMessage(), e);
            }
        }
    }

    @Override
    public void startServe() throws IOException {
        this.metadata = new Metadata();
        stub = (Namenode) UnicastRemoteObject.exportObject(this, 0);
        registry = LocateRegistry.createRegistry(Catalog.SDFS_NAMENODE_PORT);
        registry.rebind("namenode", stub);
        scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(new CheckReplication(), Catalog.SAFE_MODE_DURATION,
                Catalog.REPLICATION_CHECK_PERIOD, Catalog.TIME_UNIT);
        synchronized (ggms.getMembershipList()) {
            ggms.addObserver(this);
        }
    }

    @Override
    public void stopServe() throws NoSuchObjectException {
        synchronized (ggms.getMembershipList()) {
            ggms.deleteObserver(this);
        }
        UnicastRemoteObject.unexportObject(registry, true);
        UnicastRemoteObject.unexportObject(stub, true);
        scheduler.shutdown();
    }

    @Override
    public void addFile(String file, InetAddress IP) throws RemoteException {
        metadata.addFile(file, IP);
    }

    @Override
    public void update(Observable o, Object arg) {
        @SuppressWarnings("unchecked")
        List<Identity> failedNodes = ((ArrayList<ArrayList<Identity>>) arg).get(0);
        if (!failedNodes.isEmpty()) {
            List<InetAddress> addresses = new ArrayList<>();
            for (Identity id : failedNodes) {
                addresses.add(id.IPAddress);
            }
            metadata.deleteNodes(addresses);
        }
    }

}
