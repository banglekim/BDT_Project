package bangle.cs523.SparkStreamProj;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * Project using Spark Stream and connected to HBase
 *
 */
public class App 
{
	private static final String TABLE_NAME = "wordcounts";
    public static void main( String[] args ) throws IOException
    {
    	// Create a Java Spark Context
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("wordCount").setMaster("local"));

		// Load our input data
		JavaRDD<String> lines = sc.textFile(args[0]);
		
		Integer threshold = Integer.parseInt(args[1]);

		// Calculate word count
		JavaPairRDD<String, Integer> counts = lines
					.flatMap(line -> Arrays.asList(line.split(" ")))
					.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
					.reduceByKey((x, y) -> x + y);
		
		// Filter out words with count < threshold
		List<Tuple2<String, Integer>> targetWordCounts = counts.filter(x -> x._2 >= threshold).collect();
		
		List<Tuple2<Character, Integer>> tempList = new ArrayList<Tuple2<Character, Integer>>();
		for (Tuple2<String, Integer> element: targetWordCounts) {
			for (char c: element._1.toCharArray()) {
				tempList.add(new Tuple2<Character, Integer>(c,element._2));
			}
		}
		
		JavaPairRDD<Character, Integer> rdd = sc.parallelizePairs(tempList);
		
		JavaPairRDD<Character, Integer> targetCharCounts = rdd.reduceByKey((x, y) -> x + y);
		
		// Connect and insert data into HBase
		Configuration config = HBaseConfiguration.create();

		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin())
		{
			HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
			HColumnDescriptor word_cf = new HColumnDescriptor("word_data");
			HColumnDescriptor count_cf = new HColumnDescriptor("count_data");
			tableDesc.addFamily(word_cf);
			tableDesc.addFamily(count_cf);

			System.out.print("Question 1. Creating table.... ");

			if (admin.tableExists(tableDesc.getTableName()))
			{
				admin.disableTable(tableDesc.getTableName());
				admin.deleteTable(tableDesc.getTableName());
			}
			admin.createTable(tableDesc);
			
			System.out.println("Adding data...");
			
			Table wcTable = connection.getTable(tableDesc.getTableName());
			
			List<Tuple2<Character, Integer>> data = targetCharCounts.collect();
			
			for (int i =0; i< data.size(); i++) {			
				Put p = new Put(Bytes.toBytes(i));
				p.addColumn(word_cf.getName(), Bytes.toBytes("Character"),Bytes.toBytes(data.get(i)._1));
			    p.addColumn(count_cf.getName(), Bytes.toBytes("Count"),Bytes.toBytes(data.get(i)._2.toString()));
			    wcTable.put(p);
			}
		    
		    System.out.println("data inserted successfully");

			System.out.println(" Done!");
			sc.close();
		}
		
    }
}
