import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.Tuple;
import backtype.storm.operation.Collector;
import twitter4j.HashtagEntity;
import twitter4j.Status;
/**
 * 
 * @author iamneha
 *emits hashtags pulled from tweets
 */
public class HashtagExtractorBolt extends BaseFunction {

  @Override
  public void execute(Tuple tuple, Collector collector) {
    //Get the tweet
    final Status status = (Status) tuple.get(0);
    //Loop through the hashtags
    for (HashtagEntity hashtag : status.getHashtagEntities()) {
      //Emit each hashtag
      collector.emit(new Values(hashtag.getText()));
    }
  }
}