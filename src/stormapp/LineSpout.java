package stormapp;
import sdfs.Client;
import sdfs.OutsideClient;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import system.Catalog;

/**
 * This spout reads data from a CSV file.
 */
public class LineSpout extends BaseRichSpout {
  private final String fileName;
  private final char separator;
  private boolean includesHeaderRow;
  private SpoutOutputCollector _collector;
  private BufferedReader reader;
  private AtomicLong linesRead;

  public LineSpout(String filename, char separator, boolean includesHeaderRow) {
    this.fileName = filename;
    this.separator = separator;
    this.includesHeaderRow = includesHeaderRow;
    linesRead=new AtomicLong(0);
  }
  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    Logger logger = null;
    try {
        Client sdfsClient = new OutsideClient(logger,Catalog.NIMBUS_ADDRESS); //edit nimbus address for storm
      sdfsClient.fetchFileFromSDFS(fileName, Catalog.CRANE_DIR + fileName); //edit dir for storm
      reader = new BufferedReader(new FileReader(Catalog.CRANE_DIR + fileName), separator); //edit dir for storm
      // read and ignore the header if one exists
      if (includesHeaderRow) reader.readLine();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void nextTuple() {
    try {
      String line = reader.readLine();
      if (line != null) {
        long id=linesRead.incrementAndGet();
        _collector.emit(new Values(line),id);
      }
      else
        System.out.println("Finished reading file, "+linesRead.get()+" lines read");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
    System.err.println("Failed tuple with id "+id);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    try {
        BufferedReader reader = new BufferedReader(new FileReader(Catalog.CRANE_DIR + fileName), separator);
      // read csv header to get field info
      String fields = reader.readLine();
      if (includesHeaderRow) {
        System.out.println("DECLARING OUTPUT FIELDS");
        System.out.println(fields);
        declarer.declare(new Fields(Arrays.asList(fields)));
      } else {
        String f="line";
        declarer.declare(new Fields(f));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}