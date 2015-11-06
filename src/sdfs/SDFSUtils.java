package sdfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import system.Catalog;

/**
 * SDFSUtils provides some utility methods used by other SDFS class.
 */
public class SDFSUtils {
    /**
     * Write the content to output stream of the socket, and close the output
     * stream of the socket after finishing.
     * 
     * @param socket
     * @param content
     * @throws IOException
     */
    public static void writeAndClose(Socket socket, byte[] content) throws IOException {
        try (OutputStream out = new BufferedOutputStream(socket.getOutputStream())) {
            out.write(content);
            out.flush();
        }
    }
    
    public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bais)) {
            return ois.readObject();
        }
    }
    
    public static byte[] serialize(Object o) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(o);
            return baos.toByteArray();
        }
    }

    /**
     * Check whether the first byte of the response from the input stream of the
     * socket is "success", and close the input stream of the socket after
     * finishing.
     * 
     * @param socket
     * @throws IOException
     *             if the first byte of the response is not the payload
     *             descriptor of "success" or there is no response.
     */
    public static void checkSuccessResponse(Socket socket) throws IOException {
        try (InputStream in = socket.getInputStream()) {
            int response = in.read();
            if (response != PayloadDescriptor.getDescriptor("success")) {
                throw new IOException();
            }
        }
    }

    /**
     * Communicate with name node via the specified command, and return the
     * response from name node. The method abstracts away the chore work to open
     * and close socket.
     * 
     * @param commandType
     *            one of "put_request", "get_request", "delete"
     * @param sdfsFileName
     *            the target file on SDFS
     * @return the response from name node
     * @throws IOException
     *             if fail to communicate with name node
     */
    public static byte[] communicateWithNameNode(InetAddress nameNode, String commandType,
            String sdfsFileName) throws IOException {
        try (Socket socket = new Socket(nameNode, Catalog.SDFS_NAMENODE_PORT)) {
            byte[] command = CommandEncoderDecoder.encode(commandType, null, sdfsFileName, null);
            writeAndClose(socket, command);
            return readAllBytes(socket);
        }
    }

    /**
     * Read all bytes from the input stream of the given socket, and close the
     * input stream of the socket after finishing.
     * 
     * @param socket
     *            an open socket to read from
     * @return all received bytes
     * @throws IOException
     *             if any error occurs.
     */
    public static byte[] readAllBytes(Socket socket) throws IOException {
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
    public static void putFile(String localFileName, String sdfsFileName, InetAddress dataNode)
            throws IOException {
        try (Socket socket = new Socket(dataNode, Catalog.SDFS_DATANODE_PORT)) {
            byte[] command = CommandEncoderDecoder.encode("put", localFileName, sdfsFileName, null);
            writeAndClose(socket, command);
            checkSuccessResponse(socket);
        }
    }
    
    /**
     * @return all files stored on SDFS on the machine
     */
    public static List<String> getSDFSFiles() {
        File[] files = new File(Catalog.SDFS_DIR).listFiles();

        List<String> fileList = new ArrayList<>();
        for (File file : files) {
            fileList.add(file.getName());
        }
        return fileList;
    }
}
