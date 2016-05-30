package org.vimox.twitter.trend;

import com.lambdaworks.redis.RedisConnection;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import twitter4j.HashtagEntity;
import twitter4j.Status;

public class HashTagExtractor extends BaseFunction {
	transient RedisConnection<String,String> redis;
  @Override
  public void execute(TridentTuple tuple, TridentCollector collector) {
    //Get the tweet
    final Status status = (Status) tuple.get(0);
    
    //Loop through the hashtags
    for (HashtagEntity hashtag : status.getHashtagEntities()) {
      //Emit each hashtag
      collector.emit(new Values(hashtag.getText()));
    }
  }
}