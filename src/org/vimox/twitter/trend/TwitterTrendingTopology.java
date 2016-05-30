package org.vimox.twitter.trend;
import storm.trident.TridentTopology;
import storm.trident.operation.Filter;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Debug;
import storm.trident.operation.builtin.FirstN;
import storm.trident.spout.IBatchSpout;
import storm.trident.testing.MemoryMapState;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import backtype.storm.StormSubmitter;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

public class TwitterTrendingTopology {
  //Build the topology
  public static StormTopology buildTopology(IBatchSpout spout) throws IOException {
	  
    final TridentTopology topology = new TridentTopology();
    //Define the topology:
    //1. spout reads tweets
    //2. HashtagExtractor emits hashtags pulled from tweets
    //3. hashtags are grouped
    //4. a count of each hashtag is created
    //5. each hashtag, and how many times it has occurefed
    //   is emitted.
   
    
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    System.out.println("Enter Topics");
    String d = br.readLine();
    List<String> l = new ArrayList<String>();
   
    String[] tokens = d.split(",");
    
    for(int i=0;i<tokens.length;i++){
		l.add(tokens[i]);
	}
    
	FileOutputStream fos= new FileOutputStream("myfile.txt");
    ObjectOutputStream oos= new ObjectOutputStream(fos);
	oos.writeObject(l);
	oos.close();
	fos.close();
    
        
   System.out.println( topology.newStream("spout", spout)
    .each(new Fields("tweet"), new HashTagExtractor(), new Fields("hashtag"))
    .groupBy(new Fields("hashtag"))
    .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
    .newValuesStream()
    .applyAssembly(new FirstN(10, "count"))
    .each(new Fields("hashtag", "count"), new Debug()));
    
    //Build and return the topology
    return topology.build();
  }

  public static void main(String[] args) throws Exception {

    final Config conf = new Config();
    final IBatchSpout spout = new TwitterSpout();
    //If no args, assume we are testing locally
    if(args.length==0) {
      //create LocalCluster and submit
      final LocalCluster local = new LocalCluster();
      try {
        local.submitTopology("hashtag-count-topology", conf, buildTopology(spout));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      //If on a real cluster, set the workers and submit
      conf.setNumWorkers(3);
      StormSubmitter.submitTopology(args[0], conf, buildTopology(spout));
    }
  }
}