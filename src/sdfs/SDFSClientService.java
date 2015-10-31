package sdfs;

import java.io.IOException;

import system.DaemonService;

/*
 * SDFSClientService provides interfaces for clients to access SDFS.
 */
public class SDFSClientService implements DaemonService {

    private LeaderElectionService les;
    
    public SDFSClientService(LeaderElectionService les) {
        this.les = les;
    }
    
    @Override
    public void startServe() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void stopServe() {
        // TODO Auto-generated method stub

    }

}
