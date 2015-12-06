package crane.demo;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import crane.INimbus;
import crane.bolt.IBolt;
import crane.bolt.OngoingEventFilterBolt;
import crane.bolt.SinkBolt;
import crane.partition.RandomPartitionStrategy;
import crane.spout.FileLineSpout;
import crane.spout.ISpout;
import crane.topology.Topology;
import system.Catalog;

public class FindTweetTopicUsers {
    public static void main(String[] args)
            throws NotBoundException, IOException, InterruptedException {
        ISpout spout = new FileLineSpout("spout", "TT-annotations.csv");
        IBolt bolt1 = new OngoingEventFilterBolt("bolt-1", 1, new RandomPartitionStrategy());
        IBolt bolt2 = new SinkBolt("bolt-2", "NULL");
        spout.addChild(bolt1);
        bolt1.addChild(bolt2);
        
        Topology top = new Topology("FindTweetTopicUsers", spout);

        Registry registry = LocateRegistry.getRegistry(Catalog.NIMBUS_ADDRESS, Catalog.NIMBUS_PORT);
        INimbus nimbus = (INimbus) registry.lookup("nimbus");

        nimbus.submitTopology(top);
    }
}
