package stormapp;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class CountTopicTopology {
    static final String TOPOLOGY_NAME = "storm-twitter-topic-count";
    public static void main(String[] args) {
        Config config = new Config();
        config.setMessageTimeoutSecs(120);
        TopologyBuilder b = new TopologyBuilder();
        b.setSpout("LineSpout", new LineSpout("TT-annotations.csv",';',false));
        b.setBolt("CountTopicBolt", new FilterBolt()).shuffleGrouping("LineSpout");
        
        final LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, b.createTopology());

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                cluster.killTopology(TOPOLOGY_NAME);
                cluster.shutdown();
            }
        });
    }
}
