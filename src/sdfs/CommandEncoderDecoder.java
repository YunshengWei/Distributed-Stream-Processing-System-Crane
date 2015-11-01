package sdfs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.NoSuchElementException;

import system.Catalog;

/**
 * CommandEncoderDecoder is used to 1) encode command to byte array, 2) extract
 * information from command byte array.
 */
public class CommandEncoderDecoder {
    public static byte[] encode(String commandType, String localFileName, String sdfsFileName,
            InetAddress dest) throws IOException {
        byte pd = PayloadDescriptor.getDescriptor(commandType);

        switch (commandType) {
        case "success":
        case "put_request":
            return new byte[] { pd };
        case "get_request":
        case "get":
        case "delete": {
            byte[] fileNameBytes = sdfsFileName.getBytes(Catalog.encoding);

            ByteBuffer bb = ByteBuffer.allocate(1 + fileNameBytes.length);
            bb.put(pd).put(fileNameBytes);
            return bb.array();
        }
        case "put": {
            Path file = Paths.get(localFileName);
            byte[] fileContentBytes = Files.readAllBytes(file);
            byte[] fileNameBytes = sdfsFileName.getBytes(Catalog.encoding);

            // the first byte is payload descriptor, the following 4 bytes is an
            // integer indicating the file size, the following is the file
            // content, all bytes after the file content are file name.
            ByteBuffer bb = ByteBuffer
                    .allocate(1 + 4 + fileContentBytes.length + fileNameBytes.length);
            bb.put(pd).putInt(fileContentBytes.length).put(fileContentBytes).put(fileNameBytes);
            return bb.array();
        }
        case "replicate": {
            byte[] fileNameBytes = sdfsFileName.getBytes(Catalog.encoding);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            new ObjectOutputStream(baos).writeObject(dest);
            byte[] destBytes = baos.toByteArray();

            ByteBuffer bb = ByteBuffer.allocate(1 + 4 + fileNameBytes.length + destBytes.length);
            bb.put(pd).putInt(fileNameBytes.length).put(fileNameBytes).put(destBytes);
            return bb.array();
        }
        }

        return null;
    }

    public static String getCommandType(byte[] command) {
        return PayloadDescriptor.getCommand(command[0]);
    }

    public static String getFileName(byte[] command) throws UnsupportedEncodingException {
        String commandType = CommandEncoderDecoder.getCommandType(command);
        switch (commandType) {
        case "get":
        case "get_request":
        case "delete":
            return new String(Arrays.copyOfRange(command, 1, command.length), Catalog.encoding);
        case "put":
        case "replicate": {
            int fileSize = ByteBuffer.wrap(command, 1, 4).getInt();
            return new String(Arrays.copyOfRange(command, 5 + fileSize, command.length),
                    Catalog.encoding);
        }
        default:
            throw new NoSuchElementException();
        }
    }

    public static byte[] getFileContent(byte[] command) {
        String commandType = CommandEncoderDecoder.getCommandType(command);
        if (commandType.equals("put")) {
            int fileSize = ByteBuffer.wrap(command, 1, 4).getInt();
            return Arrays.copyOfRange(command, 5, 5 + fileSize);
        } else {
            throw new NoSuchElementException();
        }
    }

    public static InetAddress getDestination(byte[] command)
            throws ClassNotFoundException, IOException {
        String commandType = CommandEncoderDecoder.getCommandType(command);
        if (commandType.equals("replicate")) {
            int fileNameLength = ByteBuffer.wrap(command, 1, 4).getInt();
            return (InetAddress) new ObjectInputStream(new ByteArrayInputStream(
                    Arrays.copyOfRange(command, 5 + fileNameLength, command.length))).readObject();
        } else {
            throw new NoSuchElementException();
        }
    }
}
