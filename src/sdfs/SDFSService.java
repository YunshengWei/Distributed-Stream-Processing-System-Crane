package sdfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Logger;

import membershipservice.GossipGroupMembershipService;
import system.Catalog;
import system.DaemonService;
import system.Identity;

public class SDFSService implements DaemonService {

    private class DataNodeWorker implements Runnable {

        @Override
        public void run() {
            try {
                while (true) {
                    Socket socket = serverSocket.accept();
                    execute(socket);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void executeGet(Socket socket, byte[] command) throws IOException {
            String fileName = new String(Arrays.copyOfRange(command, 1, command.length),
                    Catalog.encoding);
            Path filePath = Paths.get(Catalog.SDFS_DIR + fileName);
            byte[] fileContent = Files.readAllBytes(filePath);
            try (BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream())) {
                out.write(fileContent);
                out.flush();
            }
        }

        private void executeDelete(Socket socket, byte[] command) throws IOException {
            String fileName = new String(Arrays.copyOfRange(command, 1, command.length));
            Path filePath = Paths.get(Catalog.SDFS_DIR + fileName);
            Files.deleteIfExists(filePath);
            try (OutputStream out = socket.getOutputStream()) {
                out.write(PayloadDescriptor.getDescriptor("success"));
            }
        }

        private void executePut(Socket socket, byte[] command) throws IOException {
            int fileSize = ByteBuffer.wrap(command, 1, 4).getInt();
            byte[] fileContent = Arrays.copyOfRange(command, 5, 5 + fileSize);
            String fileName = new String(Arrays.copyOfRange(command, 5 + fileSize, command.length),
                    Catalog.encoding);
            Path filePath = Paths.get(Catalog.SDFS_DIR + fileName);
            try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(filePath))) {
                out.write(fileContent);
                out.flush();
            }
            try (OutputStream out = socket.getOutputStream()) {
                out.write(PayloadDescriptor.getDescriptor("success"));
            }
        }

        private void executeReplicate(Socket socket, byte[] command)
                throws IOException, ClassNotFoundException {
            int fileNameLength = ByteBuffer.wrap(command, 1, 4).getInt();
            String fileName = new String(Arrays.copyOfRange(command, 5, 5 + fileNameLength),
                    Catalog.encoding);
            InetAddress IPAddress = (InetAddress) new ObjectInputStream(new ByteArrayInputStream(
                    Arrays.copyOfRange(command, 5 + fileNameLength, command.length))).readObject();

            putFile(Catalog.SDFS_DIR + fileName, fileName, IPAddress);
        }

        private void execute(Socket socket) throws IOException, ClassNotFoundException {
            try {
                byte[] command = readAllBytes(socket);
                String commandType = PayloadDescriptor.getCommand(command[0]);
                switch (commandType) {
                case "get":
                    executeGet(socket, command);
                    break;
                case "delete":
                    executeDelete(socket, command);
                    break;
                case "put":
                    executePut(socket, command);
                    break;
                case "replicate":
                    executeReplicate(socket, command);
                    break;
                }

            } finally {
                socket.close();
            }
        }

    }

    private GossipGroupMembershipService ggms;
    private ServerSocket serverSocket;
    private Identity nameNode;

    private final static Logger LOGGER = initializeLogger();

    private static Logger initializeLogger() {
        return null;
    }

    public Identity getNameNode() {
        return nameNode;
    }

    private byte[] communicateWithNameNode(String commandType, String sdfsFileName)
            throws IOException {
        Socket socket = new Socket(nameNode.IPAddress, Catalog.SDFS_NAMENODE_PORT);

        try (OutputStream out = socket.getOutputStream()) {
            switch (commandType) {
            case "put_request": {
                out.write(PayloadDescriptor.getDescriptor("put_request"));
                return readAllBytes(socket);
            }
            case "delete": {
                byte[] fileNameBytes = sdfsFileName.getBytes(Catalog.encoding);
                ByteBuffer bb = ByteBuffer.allocate(1 + fileNameBytes.length);
                bb.put(PayloadDescriptor.getDescriptor("delete"));
                bb.put(fileNameBytes);
                out.write(bb.array());
                return null;
            }
            case "get_request": {
                byte[] fileNameBytes = sdfsFileName.getBytes(Catalog.encoding);
                ByteBuffer bb = ByteBuffer.allocate(1 + fileNameBytes.length);
                bb.put(PayloadDescriptor.getDescriptor("get_request"));
                bb.put(fileNameBytes);
                out.write(bb.array());
                return readAllBytes(socket);
            }
            }
        } finally {
            socket.close();
        }

        return null;
    }

    private byte[] readAllBytes(Socket socket) throws IOException {
        try (BufferedInputStream in = new BufferedInputStream(socket.getInputStream())) {
            byte[] tmp = new byte[1024];
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            while (true) {
                int n = in.read(tmp);
                if (n < 0) {
                    break;
                } else {
                    baos.write(tmp, 0, n);
                }
            }
            return baos.toByteArray();
        }
    }

    /**
     * Put file on this data node to another data node. File can be either on
     * SDFS or on local FS.
     * 
     * @param localFileName
     *            the file to transfer (including path relative to root
     *            directory, not SDFS_DIR)
     * @param sdfsFileName
     *            the name of the file on SDFS
     * @param dataNode
     *            the destination of the file
     * @throws IOException
     */
    private void putFile(String localFileName, String sdfsFileName, InetAddress dataNode)
            throws IOException {
        Socket socket = new Socket(dataNode, Catalog.SDFS_DATANODE_PORT);
        try (OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream()) {
            Path file = Paths.get(localFileName);
            byte[] fileContentBytes = Files.readAllBytes(file);
            byte[] fileNameBytes = sdfsFileName.getBytes(Catalog.encoding);
            // the first byte is payload descriptor, the following 4 bytes is an
            // integer indicating the file size, all bytes after the file
            // content are file name.
            ByteBuffer bb = ByteBuffer
                    .allocate(1 + 4 + fileContentBytes.length + fileNameBytes.length);

            bb.put(PayloadDescriptor.getDescriptor("put"));
            bb.putInt(fileContentBytes.length);
            bb.put(fileContentBytes);
            bb.put(fileNameBytes);
            out.write(bb.array());

            int response = socket.getInputStream().read();
            if (response != PayloadDescriptor.getDescriptor("success")) {
                throw new IOException();
            }

        } finally {
            socket.close();
        }
    }

    /**
     * @return a list of file names stored on SDFS on the machine
     */
    public List<String> getSDFSFiles() {
        File sdfs_dir = new File(Catalog.SDFS_DIR);
        File[] files = sdfs_dir.listFiles();

        List<String> fileList = new ArrayList<>();
        for (File file : files) {
            fileList.add(file.getName());
        }
        return fileList;
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

        @SuppressWarnings("unchecked")
        List<InetAddress> dataNodes = (ArrayList<InetAddress>) new ObjectInputStream(
                new ByteArrayInputStream(communicateWithNameNode("put_request", null)))
                        .readObject();
        for (InetAddress dataNode : dataNodes) {
            putFile(localFileName, sdfsFileName, dataNode);
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
        communicateWithNameNode("delete", sdfsFileName);
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

        byte[] fileNameBytes = sdfsFileName.getBytes(Catalog.encoding);
        ByteBuffer bb = ByteBuffer.allocate(1 + fileNameBytes.length);
        bb.put(PayloadDescriptor.getDescriptor("get"));
        bb.put(fileNameBytes);
        byte[] command = bb.array();

        for (InetAddress dataNode : dataNodes) {
            try {
                Socket socket2 = new Socket(dataNode, Catalog.SDFS_DATANODE_PORT);
                socket2.getOutputStream().write(command);

                byte[] fileContent = readAllBytes(socket2);
                Path filePath = Paths.get(Catalog.SDFS_DIR + localFileName);
                try (OutputStream out2 = new BufferedOutputStream(
                        Files.newOutputStream(filePath))) {
                    out2.write(fileContent);
                    out2.flush();
                }
                return;
            } catch (Exception e) {
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
        return (ArrayList<InetAddress>) new ObjectInputStream(
                new ByteArrayInputStream(communicateWithNameNode("get_request", sdfsFileName)))
                        .readObject();
    }

    public SDFSService(InetAddress introducerIP) {
        ggms = new GossipGroupMembershipService(introducerIP);
    }

    @Override
    public void startServe() throws IOException {
        ggms.startServe();
        serverSocket = new ServerSocket(Catalog.SDFS_DATANODE_PORT);
        new Thread(new DataNodeWorker()).start();
    }

    @Override
    public void stopServe() {
        ggms.stopServe();

    }

    public static void main(String[] args) throws IOException {
        SDFSService sdfss = new SDFSService(InetAddress.getByName(Catalog.INTRODUCER_ADDRESS));
        sdfss.startServe();

        Scanner in = new Scanner(System.in);
        String line;
        while ((line = in.nextLine()) != null) {
            if (line.equals("Leave group")) {
                sdfss.ggms.stopServe();
            } else if (line.equals("Join group")) {
                sdfss.ggms.startServe();
            } else if (line.equals("Show membership list")) {
                System.out.println(sdfss.ggms.getMembershipList());
            } else if (line.equals("Show self id")) {
                System.out.println(sdfss.ggms.getSelfId());
            } else if (line.startsWith("put")) {
                String[] parts = line.split("\\s+");
                String localFileName = parts[1];
                String sdfsFileName = parts[2];
                // TODO
            } else if (line.startsWith("get")) {
                String[] parts = line.split("\\s+");
                String sdfsFileName = parts[1];
                String localFileName = parts[2];
                // TODO
            } else if (line.startsWith("delete")) {
                String[] parts = line.split("\\s+");
                String sdfsFileName = parts[1];
                // TODO
            } else if (line.equals("store")) {
                System.out.println(sdfss.getSDFSFiles());
            } else if (line.startsWith("list")) {
                String[] parts = line.split("\\s+");
                String sdfsFileName = parts[1];
                // TODO
            }
        }
        in.close();
    }
}
