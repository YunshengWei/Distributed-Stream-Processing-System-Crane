import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Debug;
import storm.trident.operation.builtin.FirstN;
import storm.trident.spout.IBatchSpout;
import storm.trident.testing.MemoryMapState;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.StormSubmitter;

import java.io.IOException;
//Define the topology:
//1. spout reads tweets
//2. HashtagExtractor emits hashtags pulled from tweets
//3. hashtags are grouped by the filters mentioned
//4. a count of each hashtag is created
//5. each hashtag, and how many times it has occured is emitted.
public class TrendingTweetsTopology {
    
    static final String TOPOLOGY_NAME = "storm-twitter-top-trending-tweets";

    public static void main(String[] args) {
        Config config = new Config();
        config.setMessageTimeoutSecs(120);
//TODO
        TopologyBuilder b = new TopologyBuilder();
        b.setSpout("TwitterSampleSpout", new TwitterSampleSpout());
        b.setBolt("FilterTopicsBolt", new FilterTopicsBolt(5)).shuffleGrouping("TwitterSampleSpout");
        b.setBolt("HashTagExtractorBolt", new HashTagExtractorBolt()).shuffleGrouping("FilterTopicsBolt");
        

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
