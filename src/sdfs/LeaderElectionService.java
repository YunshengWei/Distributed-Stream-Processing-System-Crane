package sdfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import membershipservice.GossipGroupMembershipService;
import system.DaemonService;
import system.Identity;
/**
 * LeaderElectionService is a class used to select a new leader
 * when the master node fails.A node which has the oldest timestamp 
 * in the membership list is selected as the leader.
 */

public class LeaderElectionService implements DaemonService {
     private GossipGroupMembershipService ggms;

    @Override
    public void startServe() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void stopServe() {
        // TODO Auto-generated method stub

    }
    
    public Identity getLeaderIdentity() {
        List<Identity> alive=ggms.getMembershipList().getAliveMembersIncludingSelf();
        Collections.sort(alive,new MyComparator());
        System.out.println("Sorted timestamp entries in ascending order");
        for(Identity timestamp:alive)
        {
          System.out.println(timestamp);
        }
        return alive.get(0); //returning node with the oldest timestamp as the leader
    }
    
    //sorting each node in ascending order on the basis of timestamp
    public class MyComparator implements Comparator<Identity>{
     @Override
     public int compare(Identity i1, Identity i2)
     {
         if(i1.timestamp>i2.timestamp)
         {
             return 1; 
         } else{
                 return -1;
             }
     }
    }
}
