package sdfs;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import membershipservice.GossipGroupMembershipService;
import system.Catalog;
import system.CustomizedFormatter;
import system.DaemonService;

public class FileSystemService implements DaemonService {

    private DatanodeService dns;
    private NamenodeService nns;
    private Client client;
    private GossipGroupMembershipService ggms;
    private LeaderElectionService les;
    private Registry registry;
    
    private final static Logger LOGGER = initializeLogger();
    
    private static Logger initializeLogger() {
        Logger logger = null;
        try {
            logger = Logger.getLogger(FileSystemService.class.getName());
            Handler fileHandler = new FileHandler(Catalog.LOG_DIR + Catalog.SDFS_LOG);
            fileHandler.setFormatter(new system.CustomizedFormatter());
            fileHandler.setLevel(Level.ALL);

           
            ConsoleHandler consoleHandler = new ConsoleHandler();
            consoleHandler.setFormatter(new CustomizedFormatter());
            consoleHandler.setLevel(Level.ALL);

            logger.addHandler(consoleHandler);
            logger.addHandler(fileHandler);
        } catch (SecurityException | IOException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            System.exit(-1);
        }
        return logger;
    }

    public FileSystemService() throws UnknownHostException {
        if (System.getSecurityManager() == null) {
            System.setSecurityManager(new SecurityManager());
        }
        ggms = new GossipGroupMembershipService(InetAddress.getByName(Catalog.INTRODUCER_ADDRESS));
        les = new LeaderElectionService(ggms, LOGGER);
        dns = new DatanodeService(les, LOGGER);
        nns = new NamenodeService(ggms, LOGGER);
        client = new Client();
    }

    @Override
    public void startServe() throws IOException {
        registry = LocateRegistry.createRegistry(Catalog.SDFS_NAMENODE_PORT);
        ggms.startServe();
        les.startServe();
        dns.startServe();
        nns.startServe();
    }

    @Override
    public void stopServe() throws Exception {
        UnicastRemoteObject.unexportObject(registry, true);
    }

    public static void main(String[] args) {
        LogManager.getLogManager().reset();
    }
}
