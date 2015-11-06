package sdfs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

import system.Catalog;
import system.DaemonService;

/**
 * SDFSNameNodeService is responsible for the the task of a name node in SDFS.
 */
public class SDFSNameNodeService implements DaemonService {

    private ServerSocket serverSocket;
    private SDFSMetadata metadata = new SDFSMetadata();

    private class SDFSNameNodeWorker implements Runnable {

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

        private void executePutRequest(Socket socket, byte[] command) throws IOException {
            List<InetAddress> nodeList = metadata.getKidlestNodes(Catalog.REPLICATION_FACTOR);
            byte[] response = SDFSUtils.serialize(nodeList);
            SDFSUtils.writeAndClose(socket, response);
        }

        private void executeGetRequest(Socket socket, byte[] command) throws IOException {
            String fileName = CommandEncoderDecoder.getFileName(command);
            List<InetAddress> nodeList = metadata.getFileLocations(fileName);
            byte[] response = SDFSUtils.serialize(nodeList);
            SDFSUtils.writeAndClose(socket, response);
        }

        private void executeDelete(Socket socket, byte[] command) throws IOException {
            String fileName = CommandEncoderDecoder.getFileName(command);
            List<InetAddress> nodeList = metadata.getFileLocations(fileName);
            metadata.deleteFile(fileName);
            for (InetAddress node : nodeList) {
                try (Socket s = new Socket(node, Catalog.SDFS_DATANODE_PORT)) {
                    SDFSUtils.writeAndClose(s, command);
                    SDFSUtils.checkSuccessResponse(socket);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            SDFSUtils.writeAndClose(socket,
                    new byte[] { PayloadDescriptor.getDescriptor("success") });
        }

        private void executeBlockReport(Socket socket, byte[] command) {

        }

        private void handle(Socket socket) throws IOException, ClassNotFoundException {
            try {
                byte[] command = SDFSUtils.readAllBytes(socket);
                switch (CommandEncoderDecoder.getCommandType(command)) {
                case "get_request":
                    executeGetRequest(socket, command);
                    break;
                case "delete":
                    executeDelete(socket, command);
                    break;
                case "put_request":
                    executePutRequest(socket, command);
                    break;
                case "blockreport":
                    executeBlockReport(socket, command);
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
