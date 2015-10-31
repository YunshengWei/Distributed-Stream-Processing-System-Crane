package sdfs;

import java.io.IOException;

import system.DaemonService;
import system.Identity;

public class LeaderElectionService implements DaemonService {

    @Override
    public void startServe() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void stopServe() {
        // TODO Auto-generated method stub

    }
    
    public Identity getLeaderIdentity() {
        return null;
    }
}
