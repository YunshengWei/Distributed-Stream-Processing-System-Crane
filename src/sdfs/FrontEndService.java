package sdfs;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Observable;
import java.util.Observer;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import membershipservice.GossipGroupMembershipService;
import system.Catalog;
import system.CustomizedFormatter;
import system.DaemonService;

/**
 * front end for SDFS
 */
public class FrontEndService implements DaemonService, Observer {

    private final GossipGroupMembershipService ggms;
    private final LeaderElectionService les;
    private final static Logger LOGGER = initializeLogger();

    private static Logger initializeLogger() {
        Logger logger = null;
        try {
            logger = Logger.getLogger(FileSystemService.class.getName());
            logger.setUseParentHandlers(false);

            Handler fileHandler = new FileHandler(Catalog.LOG_DIR + Catalog.SDFS_LOG);
            fileHandler.setFormatter(new system.CustomizedFormatter());
            fileHandler.setLevel(Level.ALL);

             ConsoleHandler consoleHandler = new ConsoleHandler();
             consoleHandler.setFormatter(new CustomizedFormatter());
             consoleHandler.setLevel(Level.ALL);
            
             logger.addHandler(consoleHandler);
            logger.addHandler(fileHandler);
        } catch (SecurityException | IOException e) {
            e.printStackTrace();
            // logger.log(Level.SEVERE, e.getMessage(), e);
            System.exit(-1);
        }
        return logger;
    }
    
    public FrontEndService() {
        ggms = new GossipGroupMembershipService(InetAddress.getByName(Catalog.INTRODUCER_ADDRESS),
                Catalog.DEFAULT_SDFS_GMS_PORT, Catalog.DEFAULT_SDFS_GMS_PORT);
        les = new LeaderElectionService(ggms, LOGGER);
        les.addObserver(this);
    }
    
    @Override
    public void startServe() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void stopServe() throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public void update(Observable o, Object arg) {
        // TODO Auto-generated method stub
        
    }

}
