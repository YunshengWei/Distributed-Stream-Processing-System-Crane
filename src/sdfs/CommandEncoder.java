package sdfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import system.Catalog;

/**
 * CommandEncoder is used to encode command to byte array.
 */
public class CommandEncoder {
    public static byte[] encode(String commandType, String... files) throws IOException {
        byte pd = PayloadDescriptor.getDescriptor(commandType);

        switch (commandType) {
        case "success":
        case "put_request":
            return new byte[] { pd };
        case "get_request":
        case "get":
        case "delete": {
            String sdfsFileName = files[0];
            byte[] fileNameBytes = sdfsFileName.getBytes(Catalog.encoding);

            ByteBuffer bb = ByteBuffer.allocate(1 + fileNameBytes.length);
            bb.put(pd);
            bb.put(fileNameBytes);
            return bb.array();
        }
        case "put": {
            String localFileName = files[0];
            String sdfsFileName = files[1];

            Path file = Paths.get(localFileName);
            byte[] fileContentBytes = Files.readAllBytes(file);
            byte[] fileNameBytes = sdfsFileName.getBytes(Catalog.encoding);

            // the first byte is payload descriptor, the following 4 bytes is an
            // integer indicating the file size, the following is the file
            // content, all bytes after the file content are file name.
            ByteBuffer bb = ByteBuffer
                    .allocate(1 + 4 + fileContentBytes.length + fileNameBytes.length);

            bb.put(pd);
            bb.putInt(fileContentBytes.length);
            bb.put(fileContentBytes);
            bb.put(fileNameBytes);

            return bb.array();
        }
        case "replicate":
            break;
        }

        return null;
    }
}
