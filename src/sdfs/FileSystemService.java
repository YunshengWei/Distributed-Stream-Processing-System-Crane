package sdfs;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Observable;
import java.util.Observer;
import java.util.Scanner;
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
        selfIP = InetAddress.getLocalHost();
        
        ggms = new GossipGroupMembershipService(InetAddress.getByName(Catalog.INTRODUCER_ADDRESS));
        les = new LeaderElectionService(ggms, LOGGER);
        dns = new DatanodeService(les, LOGGER);
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
        les.deleteObserver(this);
        if (nns != null) {
            nns.stopServe();
        }
        dns.stopServe();
        les.stopServe();
        ggms.stopServe();
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
    
    public static void main(String[] args) throws Exception {
        LogManager.getLogManager().reset();
        
        FileSystemService fss = new FileSystemService();
        fss.startServe();
        Client client = fss.getClient();
        
        Scanner in = new Scanner(System.in);
        String line;
        while ((line = in.nextLine()) != null) {
            if (line.equals("Leave group")) {
                fss.stopServe();
            } else if (line.equals("Join group")) {
                fss.startServe();
            } else if (line.equals("Show membership list")) {
                System.out.println(fss.ggms.getMembershipList());
            } else if (line.equals("Show self id")) {
                System.out.println(fss.ggms.getSelfId());
            } else if (line.startsWith("put")) {
                String[] parts = line.split("\\s+");
                String localFileName = parts[1];
                String sdfsFileName = parts[2];
                client.putFileOnSDFS(localFileName, sdfsFileName);
            } else if (line.startsWith("get")) {
                String[] parts = line.split("\\s+");
                String sdfsFileName = parts[1];
                String localFileName = parts[2];
                client.fetchFileFromSDFS(sdfsFileName, localFileName);
            } else if (line.startsWith("delete")) {
                String[] parts = line.split("\\s+");
                String sdfsFileName = parts[1];
                client.deleteFileFromSDFS(sdfsFileName);
            } else if (line.equals("store")) {
                System.out.println(client.getSDFSFiles());
            } else if (line.startsWith("list")) {
                String[] parts = line.split("\\s+");
                String sdfsFileName = parts[1];
                System.out.println(client.getFileLocations(sdfsFileName));
            }
        }
        in.close();
    }
}
