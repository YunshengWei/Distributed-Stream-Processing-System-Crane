package sdfs;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

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
            String fileName = CommandEncoderDecoder.getFileName(command);
            Path filePath = Paths.get(Catalog.SDFS_DIR + fileName);
            byte[] fileContent = Files.readAllBytes(filePath);
            SDFSUtils.writeAndClose(socket, fileContent);
        }

        private void executeDelete(Socket socket, byte[] command) throws IOException {
            String fileName = CommandEncoderDecoder.getFileName(command);
            Path filePath = Paths.get(Catalog.SDFS_DIR + fileName);
            Files.deleteIfExists(filePath);
            SDFSUtils.writeAndClose(socket,
                    new byte[] { PayloadDescriptor.getDescriptor("success") });
        }

        private void executePut(Socket socket, byte[] command) throws IOException {
            byte[] fileContent = CommandEncoderDecoder.getFileContent(command);
            String fileName = CommandEncoderDecoder.getFileName(command);

            Path filePath = Paths.get(Catalog.SDFS_DIR + fileName);
            try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(filePath))) {
                out.write(fileContent);
                out.flush();
            }
            SDFSUtils.writeAndClose(socket,
                    new byte[] { PayloadDescriptor.getDescriptor("success") });
        }

        private void executeReplicate(Socket socket, byte[] command)
                throws IOException, ClassNotFoundException {
            String fileName = CommandEncoderDecoder.getFileName(command);
            InetAddress dest = CommandEncoderDecoder.getDestination(command);
            SDFSUtils.putFile(Catalog.SDFS_DIR + fileName, fileName, dest);
        }

        private void handle(Socket socket) throws IOException, ClassNotFoundException {
            try {
                byte[] command = SDFSUtils.readAllBytes(socket);
                switch (CommandEncoderDecoder.getCommandType(command)) {
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
                default:
                    throw new UnsupportedOperationException();
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
