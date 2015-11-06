package sdfs;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
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

import system.Catalog;
import system.DaemonService;
import system.Identity;

public class DatanodeService implements DaemonService, Datanode, Observer {

    private LeaderElectionService les;
    private final InetAddress selfIP;
    private Namenode namenode;
    private final Logger logger;
    private Datanode stub;
    private ScheduledExecutorService scheduler;

    public DatanodeService(LeaderElectionService les, Logger logger) throws UnknownHostException {
        this.les = les;
        this.logger = logger;
        this.namenode = null;
        this.selfIP = InetAddress.getLocalHost();
    }

    @Override
    public void putFile(String fileName, byte[] fileContent) throws RemoteException, IOException {
        Path filePath = Paths.get(Catalog.SDFS_DIR, fileName);
        Files.write(filePath, fileContent);
        logger.info(String.format("Put file %s on SDFS.", fileName));
        namenode.addFile(fileName, selfIP);
    }

    @Override
    public void deleteFile(String fileName) throws RemoteException, IOException {
        Path filePath = Paths.get(Catalog.SDFS_DIR, fileName);
        Files.deleteIfExists(filePath);
        logger.info(String.format("Delete file %s from SDFS.", fileName));
    }

    @Override
    public byte[] getFile(String fileName) throws RemoteException, IOException {
        Path filePath = Paths.get(Catalog.SDFS_DIR, fileName);
        logger.info(String.format("Get file %s from SDFS.", fileName));
        return Files.readAllBytes(filePath);
    }

    @Override
    public void replicateFile(String fileName, Datanode... datanodes)
            throws RemoteException, IOException {
        logger.info(String.format("Receive request to replicate file %s to %s nodes.", fileName,
                datanodes.length));
        Path filePath = Paths.get(Catalog.SDFS_DIR, fileName);
        byte[] fileContent = Files.readAllBytes(filePath);
        for (Datanode datanode : datanodes) {
            datanode.putFile(fileName, fileContent);
        }
    }

    private void sendBlockReport(Namenode namenode) throws RemoteException {
        File sdfsFolder = new File(Catalog.SDFS_DIR);
        File[] files = sdfsFolder.listFiles();
        List<String> fileList = new ArrayList<>();
        for (File file : files) {
            String fileName = file.getName();
            // Assume names of hidden file start with "."
            if (!fileName.startsWith(".")) {
                fileList.add(file.getName());
            }
        }
        BlockReport blockreport = new BlockReport(selfIP, stub, fileList);
        namenode.updateMetadate(blockreport);
    }
    
    @Override
    public void startServe() throws IOException {      
        logger.info("Start datanode service.");
        stub = (Datanode) UnicastRemoteObject.exportObject(this, Catalog.SDFS_DATANODE_PORT);
        scheduler = Executors.newScheduledThreadPool(1);
        // TODO Something to do here
        les.addObserver(this);
        update(les, les.getLeader());
    }

    @Override
    public void stopServe() throws NoSuchObjectException {
        UnicastRemoteObject.unexportObject(stub, true);
        les.deleteObserver(this);
        scheduler.shutdown();
    }

    @Override
    public void update(Observable o, Object arg) {
        Identity leader = (Identity) arg;
        scheduler.schedule(new Runnable() {
            @Override
            public void run() {
                try {
                    Registry registry = LocateRegistry.getRegistry(leader.IPAddress.toString(),
                            Catalog.SDFS_NAMENODE_PORT);
                    namenode = (Namenode) registry.lookup("namenode");
                    sendBlockReport(namenode);
                } catch (RemoteException | NotBoundException e) {
                    logger.log(Level.SEVERE, e.getMessage(), e);
                }
            }
        }, Catalog.BLOCKREPORT_DELAY, Catalog.TIME_UNIT);
    }
}
