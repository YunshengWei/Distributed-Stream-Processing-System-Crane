package sdfs;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import system.Catalog;
import system.DaemonService;

/**
 * SDFSClientService provides interfaces for clients to access SDFS.
 */
public class SDFSClientService implements DaemonService {

    private LeaderElectionService les;

    public SDFSClientService(LeaderElectionService les) {
        this.les = les;
    }

    @Override
    public void startServe() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void stopServe() {
        // TODO Auto-generated method stub

    }

    /**
     * put local file onto SDFS.
     * 
     * @param localFileName
     * @param sdfsFileName
     * @throws IOExcpetion
     *             if operation does not succeed.
     * @throws ClassNotFoundException
     */
    public void putFileOnSDFS(String localFileName, String sdfsFileName)
            throws IOException, ClassNotFoundException {

        byte[] dataNodesBytes = SDFSUtils.communicateWithNameNode(les.getLeaderIdentity().IPAddress,
                "put_request", null);
        @SuppressWarnings("unchecked")
        List<InetAddress> dataNodes = (ArrayList<InetAddress>) new ObjectInputStream(
                new ByteArrayInputStream(dataNodesBytes)).readObject();
        for (InetAddress dataNode : dataNodes) {
            SDFSUtils.putFile(localFileName, sdfsFileName, dataNode);
        }
    }

    /**
     * Delete file from SDFS.
     * 
     * @param sdfsFileName
     * @throws IOException
     *             if operation does not succeed.
     */
    public void deleteFileFromSDFS(String sdfsFileName) throws IOException {
        SDFSUtils.communicateWithNameNode(les.getLeaderIdentity().IPAddress, "delete",
                sdfsFileName);
    }

    /**
     * Fetch file from SDFS.
     * 
     * @param sdfsFileName
     * @param localFileName
     * @throws IOException
     *             if operation does not succeed.
     * @throws ClassNotFoundException
     */
    public void fetchFromSDFS(String sdfsFileName, String localFileName)
            throws IOException, ClassNotFoundException {

        List<InetAddress> dataNodes = listFileLocations(sdfsFileName);
        byte[] command = CommandEncoderDecoder.encode("get", null, sdfsFileName, null);

        for (InetAddress dataNode : dataNodes) {
            try (Socket socket = new Socket(dataNode, Catalog.SDFS_DATANODE_PORT)) {
                SDFSUtils.writeAndClose(socket, command);

                byte[] fileContent = SDFSUtils.readAllBytes(socket);
                Path filePath = Paths.get(Catalog.SDFS_DIR + localFileName);
                try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(filePath))) {
                    out.write(fileContent);
                    out.flush();
                }
                return;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        throw new IOException();
    }

    /**
     * List all machines where the file is currently replicated on SDFS.
     * 
     * @param sdfsFileName
     * @return the IP address of all machines that store the file
     */
    @SuppressWarnings("unchecked")
    public List<InetAddress> listFileLocations(String sdfsFileName)
            throws IOException, ClassNotFoundException {
        byte[] dataNodesBytes = SDFSUtils.communicateWithNameNode(les.getLeaderIdentity().IPAddress,
                "get_request", sdfsFileName);
        return (ArrayList<InetAddress>) new ObjectInputStream(
                new ByteArrayInputStream(dataNodesBytes)).readObject();
    }
    
    /**
     * @return a list of file names stored on SDFS on the machine
     */
    public List<String> getSDFSFiles() {
        return SDFSUtils.getSDFSFiles();
    }
}
