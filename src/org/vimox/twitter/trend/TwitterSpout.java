package org.vimox.twitter.trend;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterSpout implements IBatchSpout {
	
  //Queue for tweets
  private LinkedBlockingQueue<Status> queue;
  //stream of tweets
  private TwitterStream twitterStream;
  
  private   AccessToken token;
  List<String> l = new ArrayList<String>();

  //open is ran when a spout instance is created
  @Override
  public void open(Map conf, TopologyContext context) {
    //Open the stream
	  
    this.twitterStream = new TwitterStreamFactory().getInstance();
    this.twitterStream.setOAuthConsumer("tKKB0EzRd36q12Bmmy5aNQVSz", "VeRh1dKq4aAL5vgDuMnqMv1Cskd4KONOIeIUyEBKVfM1kOgSZq");
    this.token= new AccessToken("280942481-lmYt5gSqR4e7CqqdYIYRpfIduA1lP8wIx65XmJ0o", "oBN2WOKFgFGUGMQoWyNnPWhw5yJr9DjjjZURLh2yFJEeX");
    twitterStream.setOAuthAccessToken(token);
    

	 try
    {
        FileInputStream fis = new FileInputStream("myfile.txt");
        ObjectInputStream ois = new ObjectInputStream(fis);
        l = (ArrayList<String>) ois.readObject();
        ois.close();
        fis.close();
     }
	 catch(IOException ioe){
         ioe.printStackTrace();
         return;
      }
	 catch(ClassNotFoundException c){
         System.out.println("Class not found");
         c.printStackTrace();
         return;
      }
    
    //Create the queue
    this.queue = new LinkedBlockingQueue<Status>();

    //Create a listener for tweets (Status)
    final StatusListener listener = new StatusListener(){
        //If there's a tweet, add to the queue
        @Override
        public void onStatus(Status status) {
          queue.offer(status);
        }

        //Everything else is empty because we
        //only care about the status (tweet)
        @Override
        public void onDeletionNotice(StatusDeletionNotice sdn) {
        }

        @Override
        public void onTrackLimitationNotice(int i) {
        }

        @Override
        public void onScrubGeo(long l, long l1) {
        }

        @Override
        public void onException(Exception e) {
        }

        @Override
        public void onStallWarning(StallWarning warning) {
        }
      };
    
    //Add the listener to the stream
    twitterStream.addListener(listener);

    //Create a filter for the topics we want
    //to find trends for
    final FilterQuery query = new FilterQuery();
    //topics
    String[] topics =  l.toArray(new String[l.size()]);
  
    query.track(topics);
    //Apply the filter
    twitterStream.filter(query);
    
  }

  //Emit tweets from the queue
  @Override
  public void emitBatch(long batchId, TridentCollector collector) {
    final Status status = queue.poll();
    if (status == null) {
      Utils.sleep(50);
    } else {
      collector.emit(new Values(status));
    }
  }

  //No handling of acks
  @Override
  public void ack(long batchId) {
  }

  //Clean up the things opened in open()
  @Override
  public void close() {
    twitterStream.shutdown();
  }

  //Get configuration
  @Override
  public Map<?, ?> getComponentConfiguration() {
    return new Config();
  }

  //Get the fields to be emitted
  @Override
  public Fields getOutputFields() {
    return new Fields("tweet");
  }
}