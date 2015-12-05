package stormapp;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

//Bolt to filter tweet based on topic
public class FilterBolt extends BaseRichBolt {
    
    private Set<String> FILTER_LIST = new HashSet<String>(Arrays.asList(new String[] {
            "ongoing-event" }));
    private OutputCollector collector;

    @Override
    public void execute(Tuple input) {
        String tweet = (String) input.getValueByField("tweet");
        String topic = (String) input.getValueByField("topic");
        if (!FILTER_LIST.contains(topic)) {
            collector.emit(new Values(tweet, topic));
        }
    }
 

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet", "topic"));
        
    }
    

}
