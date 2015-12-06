package crane.spout;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import crane.tuple.ITuple;
import crane.tuple.OneStringTuple;

public class FileLineSpout extends BasicSpout {

    private static final long serialVersionUID = 1L;

    private final String fileName;
    private transient BufferedReader reader;

    public FileLineSpout(String componentID, String fileName) {
        super(componentID);
        this.fileName = fileName;
    }

    @Override
    public void open() throws IOException {
        reader = new BufferedReader(new FileReader(fileName));
        // skip the first line
        reader.readLine();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    public ITuple nextTuple() throws IOException {
        String line = reader.readLine();
        if (line != null) {
            OneStringTuple tuple = new OneStringTuple(0, line);
            return tuple;
        } else {
            return null;
        }
    }
}
