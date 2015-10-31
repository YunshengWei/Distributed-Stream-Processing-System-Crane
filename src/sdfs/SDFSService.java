package sdfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
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

        private byte[] readCommand(Socket socket) throws IOException {
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
            Socket socket2 = connectNode(IPAddress, Catalog.SDFS_DATANODE_PORT);

        }

        private void execute(Socket socket) throws IOException, ClassNotFoundException {
            try {
                byte[] command = readCommand(socket);
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

    /**
     * Replicate file on this data node to another data node. File can be either
     * on SDFS or on local FS.
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
    private void replicateFile(String localFileName, String sdfsFileName, Identity dataNode)
            throws IOException {
        Socket socket = connectNode(dataNode, Catalog.SDFS_DATANODE_PORT);
        if (socket != null) {
            try (OutputStream out = socket.getOutputStream();
                    Scanner in = new Scanner(socket.getInputStream(), Catalog.encoding)) {
                Path file = Paths.get(localFileName);
                byte[] fileContentBytes = Files.readAllBytes(file);
                byte[] fileNameBytes = sdfsFileName.getBytes(Catalog.encoding);
                // the first 4 bytes is an integer indicating the file size,
                // all bytes after the file content are file name.
                ByteBuffer bb = ByteBuffer
                        .allocate(4 + fileContentBytes.length + fileNameBytes.length);

                bb.putInt(fileContentBytes.length);
                bb.put(fileContentBytes);
                bb.put(fileNameBytes);
                out.write(bb.array());

                if (!in.hasNextLine() || !in.nextLine().equals("success")) {
                    throw new IOException();
                }
            } finally {
                closeSocket(socket);
            }
        }
    }

    /**
     * Fetch file from SDFS.
     * 
     * @param fileName
     *            the file to fetch from SDFS
     * @param dataNode
     *            the data node from which to fetch the specified file. The file
     *            must appear on the data node. (We ignore a lot of extreme
     *            cases! Otherwise it will be too complicated!)
     * @return the content of the file as a byte array if success, otherwise
     *         <code>null</code>.
     */
    private byte[] fetchFromDataNode(String fileName, Identity dataNode) {
        Socket socket = connectNode(dataNode, Catalog.SDFS_DATANODE_PORT);
        if (socket != null) {

        }

        return null;
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
     * Connect to the specified IP address and port.
     * 
     * @param ip
     *            the IP address to connect to
     * @param port
     *            the port number
     * @return the connected socket if success, else <code>null</code>
     */
    private Socket connectNode(InetAddress ip, int port) {
        try {
            Socket socket = new Socket(ip, port);
            System.err.format("<%s:%s>: Connection set up successfully.%n", ip, port);
            return socket;
        } catch (IOException e) {
            System.err.format("<%s:%s>: Failed to establish connection.%n", ip, port);
            return null;
        }
    }

    /** Close the given socket. Require the socket being open. */
    private void closeSocket(Socket socket) {
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            System.err.format("<%s:%s>: Connection closed", socket.getInetAddress(),
                    socket.getPort());
        }
    }

    public boolean putFileOnSDFS(String localFileName, String sdfsFileName) {
        Socket socket = connectNode(nameNode, Catalog.SDFS_NAMENODE_PORT);
        if (socket != null) {

            try (PrintWriter pw = new PrintWriter(
                    new OutputStreamWriter(socket.getOutputStream(), Catalog.encoding), true)) {
                pw.println("put");
                @SuppressWarnings("unchecked")
                List<Identity> dataNodes = (ArrayList<Identity>) new ObjectInputStream(
                        socket.getInputStream()).readObject();
                for (Identity dataNode : dataNodes) {
                    replicateFile(localFileName, sdfsFileName, dataNode);
                }
                return true;
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return false;
    }

    public boolean deleteFileFromSDFS(String sdfsFileName) {
        Socket socket = connectNode(nameNode, Catalog.SDFS_NAMENODE_PORT);
        if (socket != null) {
            try (PrintWriter pw = new PrintWriter(
                    new OutputStreamWriter(socket.getOutputStream(), Catalog.encoding), true)) {
                pw.format("delete %s%n", sdfsFileName);
                Scanner sc = new Scanner(socket.getInputStream(), Catalog.encoding);
                String response = sc.nextLine();
                sc.close();
                if (response.equals("success")) {
                    return true;
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return false;
    }

    public void fetchFromSDFS(String sdfsFileName, String localFileName) {

    }

    public List<String> listFileLocations() {
        return null;
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
