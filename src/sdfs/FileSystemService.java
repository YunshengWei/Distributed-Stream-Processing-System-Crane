package sdfs;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Observable;
import java.util.Observer;
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
import system.Identity;

public class FileSystemService implements DaemonService, Observer {

    private DatanodeService dns;
    private NamenodeService nns;
    private Client client;
    private GossipGroupMembershipService ggms;
    private LeaderElectionService les;
    private final InetAddress selfIP;
    
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
        selfIP = ggms.getSelfId().IPAddress;
        les = new LeaderElectionService(ggms, LOGGER);
        dns = new DatanodeService(les, LOGGER);
        if (les.getLeader().IPAddress.equals(selfIP)) {
            update(les, les.getLeader());
        }
        les.addObserver(this);
        client = new Client(les);
    }

    @Override
    public void startServe() throws IOException {
        ggms.startServe();
        les.startServe();
        dns.startServe();
        nns.startServe();
    }

    @Override
    public void stopServe() throws Exception {
        if (nns != null) {
            nns.toString();
        }
        dns.stopServe();
        les.stopServe();
        ggms.stopServe();
        les.deleteObserver(this);
    }

    @Override
    public void update(Observable o, Object arg) {
        Identity leader = (Identity) arg;
        if (leader.IPAddress.equals(selfIP)) {
            nns = new NamenodeService(ggms, LOGGER);
            try {
                nns.startServe();
            } catch (IOException e) {
                e.printStackTrace();
                // "namenode can not start" is a fatal exception
                System.exit(-1);
            }
        }
    }
    
    public Client getClient() {
        return client;
    }
    
    public static void main(String[] args) throws IOException {
        LogManager.getLogManager().reset();
        
        FileSystemService fss = new FileSystemService();
        fss.startServe();
        Client client = fss.getClient();
    }
}
