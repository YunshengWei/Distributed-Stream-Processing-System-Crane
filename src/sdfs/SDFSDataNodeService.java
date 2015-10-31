package sdfs;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import system.Catalog;
import system.DaemonService;

/**
 * SDFSDataNodeService is responsible for the the task of a data node in SDFS.
 */
public class SDFSDataNodeService implements DaemonService {

    private LeaderElectionService les;
    private ServerSocket serverSocket;
    
    public SDFSDataNodeService(LeaderElectionService les) {
        this.les = les;
    }
    
    private class DataNodeWorker implements Runnable {

        @Override
        public void run() {
            try {
                while (true) {
                    Socket socket = serverSocket.accept();
                    handle(socket);
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

        private void handle(Socket socket) throws IOException, ClassNotFoundException {
            try {
                byte[] command = SDFSUtils.readAllBytes(socket);
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
    
    @Override
    public void startServe() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void stopServe() {
        // TODO Auto-generated method stub

    }

}
