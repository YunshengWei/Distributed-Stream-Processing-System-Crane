package sdfs;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import system.Catalog;

/**
 * Client encapsulates client operations for SDFS.
 */
public class Client {
    private final LeaderElectionService les;
    private final Logger logger;

    public Client(LeaderElectionService les, Logger logger) {
        this.les = les;
        this.logger = logger;
    }

    private Namenode getNamenode() throws RemoteException, NotBoundException {
        Registry registry = LocateRegistry.getRegistry(les.getLeader().IPAddress.getHostAddress(),
                Catalog.SDFS_NAMENODE_PORT);
        return (Namenode) registry.lookup("namenode");
    }

    public void putFileOnSDFS(String localFile, String sdfsFile)
            throws NotBoundException, IOException {
        Namenode namenode = getNamenode();
        List<Datanode> datanodes = namenode.putRequest();
        Path filePath = Paths.get(localFile);
        byte[] fileContent = Files.readAllBytes(filePath);
        for (Datanode datanode : datanodes) {
            datanode.putFile(sdfsFile, fileContent);
        }
        logger.info(String.format("Successfully put %s on SDFS %s.", localFile, sdfsFile));
    }

    public void deleteFileFromSDFS(String file) throws NotBoundException, IOException {
        Namenode namenode = getNamenode();
        namenode.deleteFile(file);
        logger.info(String.format("Successfully deleted %s from SDFS.", file));
    }

    public void fetchFileFromSDFS(String sdfsFile, String localFile)
            throws RemoteException, NotBoundException {
        Namenode namenode = getNamenode();
        List<Datanode> datanodes = namenode.getFileLocations(sdfsFile);
        for (Datanode datanode : datanodes) {
            try {
                byte[] fileContent = datanode.getFile(sdfsFile);
                Path filePath = Paths.get(localFile);
                Files.write(filePath, fileContent);
                logger.info(
                        String.format("Successfully get %s from SDFS to %s.", sdfsFile, localFile));
                return;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        throw new RemoteException();
    }

    public List<InetAddress> getFileLocations(String file)
            throws RemoteException, NotBoundException {
        Namenode namenode = getNamenode();
        return namenode.getFileLocationIPs(file);
    }

    public List<String> getSDFSFiles() {
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
        return fileList;
    }
}
