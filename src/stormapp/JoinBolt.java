package stormapp;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class JoinBolt extends BaseRichBolt{
    OutputCollector collector;

    @Override
    public void execute(Tuple input) {
        String tweet = (String) input.getValueByField("tweet");
        String topic = (String) input.getValueByField("topic");
            collector.emit(new Values(tweet, topic));
        
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
