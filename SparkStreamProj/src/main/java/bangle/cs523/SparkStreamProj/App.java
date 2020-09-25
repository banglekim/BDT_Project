package bangle.cs523.SparkStreamProj;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.api.java.JavaSparkContext;

import kafka.serializer.StringDecoder;

/**
 * Project using Spark Stream, connects to Kafka to get data from, and HBase to store analyzed data
 * collect numbers from source and classify if that number is even or odd
 */
public class App 
{
	private static final String TABLE_NAME = "numbers";
    public static void main( String[] args ) throws IOException, InterruptedException
    {
    	// Create Kafka connection
        SparkConf conf = new SparkConf()
        .setAppName("kafka-consumer")
        .setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));
		
		Map<String,String> kafkaParams = new HashMap<String,String>();
		kafkaParams.put("metadata.broker.list", "localhost:9092");
		
		Set<String> topics = Collections.singleton("KafkaData");
		
		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc,String.class,
		String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
		
		JavaDStream<String> lines = directKafkaStream.map(line -> {return line._2();});
		
		// Connect and insert data into HBase
		Configuration config = HBaseConfiguration.create();

		Connection connection = ConnectionFactory.createConnection(config);
		Admin admin = connection.getAdmin();

		HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
		HColumnDescriptor number_cf = new HColumnDescriptor("number");
		HColumnDescriptor type_cf = new HColumnDescriptor("even_odd");
		tableDesc.addFamily(number_cf);
		tableDesc.addFamily(type_cf);

		System.out.print("Creating HBase table.... ");

		if (admin.tableExists(tableDesc.getTableName()))
		{
			admin.disableTable(tableDesc.getTableName());
			admin.deleteTable(tableDesc.getTableName());
		}
		admin.createTable(tableDesc);
		Table dtTable = connection.getTable(tableDesc.getTableName());
		
		int[] index= {0};			
		lines.foreachRDD(rdd -> {
		    rdd.collect().forEach(line -> {	
				try {
					Put p = new Put(Bytes.toBytes(index[0]));
			    	if (Integer.parseInt(line)%2 == 0) {
			    		p.addColumn(type_cf.getName(), Bytes.toBytes("even_odd"),Bytes.toBytes("even number"));
			    	} else {
			    		p.addColumn(type_cf.getName(), Bytes.toBytes("even_odd"),Bytes.toBytes("odd number"));	
			    	}
				    p.addColumn(number_cf.getName(), Bytes.toBytes("number"),Bytes.toBytes(line));
				    dtTable.put(p);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    	index[0]++ ;
			});
		});
		
		ssc.start();
		ssc.awaitTermination();
    }
}

/*
package bangle.cs523.SparkStreamProj;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.api.java.JavaSparkContext;

import kafka.serializer.StringDecoder;

/**
 * Project using Spark Stream, connects to Kafka to get data from, and HBase to store analyzed data
 * collect twitts discussing about Trump or Biden to see how people think and prepare for 2020 election
 */
/*public class App 
{
	private static final String TABLE_NAME = "2020ElectionData";
    public static void main( String[] args ) throws IOException, InterruptedException
    {
    	// Create Kafka connection
        SparkConf conf = new SparkConf()
        .setAppName("kafka-consumer")
        .setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));
		
		Map<String,String> kafkaParams = new HashMap<String,String>();
		kafkaParams.put("metadata.broker.list", "localhost:9092");
		
		Set<String> topics = Collections.singleton("KafkaData");
		
		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc,String.class,
		String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
		
		// filter twitts relating to Trump or Biden
		JavaDStream<String> lines = directKafkaStream.map(line -> {return line._2();}).filter(line -> (line.contains("Trump") || line.contains("Biden")));
		
		// Connect and insert data into HBase
		Configuration config = HBaseConfiguration.create();

		Connection connection = ConnectionFactory.createConnection(config);
		Admin admin = connection.getAdmin();

		HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
		HColumnDescriptor name_cf = new HColumnDescriptor("name");
		HColumnDescriptor data_cf = new HColumnDescriptor("data");
		tableDesc.addFamily(name_cf);
		tableDesc.addFamily(data_cf);

		System.out.print("Creating HBase table.... ");

		if (admin.tableExists(tableDesc.getTableName()))
		{
			admin.disableTable(tableDesc.getTableName());
			admin.deleteTable(tableDesc.getTableName());
		}
		admin.createTable(tableDesc);
		Table dtTable = connection.getTable(tableDesc.getTableName());
		
		int[] index= {0};			
		lines.foreachRDD(rdd -> {
		    rdd.collect().forEach(line -> {	
				try {
					Put p = new Put(Bytes.toBytes(index[0]));
			    	if (line.contains("Trump")) {
			    		p.addColumn(Bytes.toBytes("name"), Bytes.toBytes("Name"),Bytes.toBytes("Trump"));
			    	} else {
			    		p.addColumn(name_cf.getName(), Bytes.toBytes("Name"),Bytes.toBytes("Biden"));	
			    	}
				    p.addColumn(data_cf.getName(), Bytes.toBytes("Twitt"),Bytes.toBytes(line));
				    dtTable.put(p);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    	index[0]++ ;
			});
		});
		
		ssc.start();
		ssc.awaitTermination();
    }
}
*/
