package bangle.cs523.KafkaProducer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Send data to Kafka project
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        final String topics = "KafkaData";
        
        // create instance for properties to access producer configs   
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");     
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);  
        props.put("linger.ms", 1);   
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
              
        for(int i = 0; i < 100; i++) {
           producer.send(new ProducerRecord<String, String>(topics, Integer.toString(i), Integer.toString(i)));
        }
        
        for(int i = 200; i < 300; i++) {
            producer.send(new ProducerRecord<String, String>(topics, Integer.toString(i), Integer.toString(i)));
        }
        
        System.out.println("Message sent successfully");
        producer.close();
    }
}


/*
package bangle.cs523.KafkaProducer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.spark.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.Status;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Integrated Spark and Twitter project
 *
 */
/*public class App 
{
    public static void main( String[] args )
    {
    	final String consumerKey = "pTU1OByDnzKIOwtf6X2rnlJcL";
        final String consumerSecret = "ST7FIBCOq5jOLd7kkmMWDSvYyLarcXpIL4jWIRDbaA1YsaX9FV";
        final String accessToken = "1307923158108790785-p1jB4FPcPdyvVH75nvH43xMhe3cEUH";
        final String accessTokenSecret = "wRjeXx1CfJhpp14n2I5c60IY768dVh04l7Gz2M7Du9CiM";
        String topics = "KafkaData";

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkTwitterProducer");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(30000));

        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);
        
        JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc);

        // Without filter: Output text of all tweets
        JavaDStream<String> statuses = twitterStream.map(status -> status.getText());        
        
        // create instance for properties to access producer configs   
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");     
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);  
        props.put("linger.ms", 1);   
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        
        statuses.foreachRDD(rdd -> {
		    rdd.foreach(line -> {	
				System.out.println(line);
				producer.send(new ProducerRecord<String, String>(topics, line));
			});
		});
        
        jssc.start();
        
        producer.close();
    }
}
*/
