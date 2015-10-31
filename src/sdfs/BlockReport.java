package sdfs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * When a new leader is elected, every data node will send block report to the
 * newly elected leader.
 */
public class BlockReport implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<String> fileList;

    private BlockReport(List<String> fileList) {
        this.fileList = fileList;
    }

    public static BlockReport buildBlockReport(String block_dir) {
        File dir = new File(block_dir);
        File[] files = dir.listFiles();
        List<String> fileList = new ArrayList<>();

        for (File file : files) {
            fileList.add(file.getName());
        }

        return new BlockReport(fileList);
    }

    public static BlockReport buildFromBytes(byte[] bytes)
            throws ClassNotFoundException, IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        BlockReport br = (BlockReport) new ObjectInputStream(bais).readObject();
        return br;
    }

    public List<String> getFileList() {
        return fileList;
    }

    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(this);
            byte[] bytes = baos.toByteArray();
            return bytes;
        }
    }
}
