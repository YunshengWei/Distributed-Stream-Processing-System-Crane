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

import system.Catalog;

/**
 * Client encapsulates client operations for SDFS.
 */
public class Client {
    private LeaderElectionService les;

    public Client(LeaderElectionService les) {
        this.les = les;
    }

    private Namenode getNamenode() throws RemoteException, NotBoundException {
        Registry registry = LocateRegistry.getRegistry(les.getLeader().IPAddress.toString(),
                Catalog.SDFS_NAMENODE_PORT);
        return (Namenode) registry.lookup("namenode");
    }

    public void putFileOnSDFS(String localFile, String sdfsFile)
            throws NotBoundException, IOException {
        Namenode namenode = getNamenode();
        List<Datanode> datanodes = namenode.putRequest();
        Path filePath = Paths.get(Catalog.SDFS_DIR, localFile);
        byte[] fileContent = Files.readAllBytes(filePath);
        for (Datanode datanode : datanodes) {
            datanode.putFile(sdfsFile, fileContent);
        }
    }

    public void deleteFileFromSDFS(String file) throws NotBoundException, IOException {
        Namenode namenode = getNamenode();
        namenode.deleteFile(file);

    }

    public void fetchFileFromSDFS(String sdfsFile, String localFile)
            throws RemoteException, NotBoundException {
        Namenode namenode = getNamenode();
        List<Datanode> datanodes = namenode.getFileLocations(sdfsFile);
        for (Datanode datanode : datanodes) {
            try {
                byte[] fileContent = datanode.getFile(sdfsFile);
                Path filePath = Paths.get(Catalog.SDFS_DIR + localFile);
                Files.write(filePath, fileContent);
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
        File[] files = new File(Catalog.SDFS_DIR).listFiles();

        List<String> fileList = new ArrayList<>();
        for (File file : files) {
            fileList.add(file.getName());
        }
        return fileList;
    }
}
