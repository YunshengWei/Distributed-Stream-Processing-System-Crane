package crane.spout;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.util.logging.Logger;

import crane.partition.IPartitionStrategy;
import crane.topology.BasicComponent;
import crane.tuple.ITuple;
import crane.tuple.OneStringTuple;
import sdfs.Client;
import sdfs.OutsideClient;
import system.Catalog;

public class FileLineSpout extends BasicComponent implements ISpout {

    private static final long serialVersionUID = 1L;

    private final String fileName;
    private transient BufferedReader reader;
    private int tupleID;

    public FileLineSpout(String componentID, IPartitionStrategy ps, String fileName) {
        super(componentID, 1, ps);
        this.fileName = fileName;
        tupleID = 0;
    }

    @Override
    public void open(Logger logger) throws IOException, NotBoundException {
        Client sdfsClient = new OutsideClient(logger, Catalog.NIMBUS_ADDRESS);
        sdfsClient.fetchFileFromSDFS(fileName, Catalog.CRANE_DIR + fileName);

        reader = new BufferedReader(new FileReader(Catalog.CRANE_DIR + fileName));
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    public ITuple nextTuple() throws IOException {
        String line = reader.readLine();
        if (line != null) {
            OneStringTuple tuple = new OneStringTuple(tupleID++, line);
            return tuple;
        } else {
            return null;
        }
    }
}
