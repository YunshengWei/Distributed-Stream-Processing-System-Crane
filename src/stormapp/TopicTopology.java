package stormapp;

import system.Catalog;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class TopicTopology {

    static final String TOPOLOGY_NAME = "storm-twitter-topic";
    public static void main(String[] args) throws Exception, InvalidTopologyException {
        Config config = new Config();
        config.setMessageTimeoutSecs(120);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("LineSpout", new LineSpout("TT-annotations.csv",';',false));
        builder.setBolt("FilterBolt", new FilterBolt()).shuffleGrouping("LineSpout");
        builder.setBolt("JoinBolt", new JoinBolt()).shuffleGrouping("FilterBolt");
        
        config.setDebug(true);


        if (args != null && args.length > 0) {
          config.setNumWorkers(3);

          StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        }
        else {
          config.setMaxTaskParallelism(3);
        final LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                cluster.killTopology(TOPOLOGY_NAME);
                cluster.shutdown();
            }
        });
    }
    }
}
