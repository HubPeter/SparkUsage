package iie.hadoop.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkStreamingTest {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName(
				"NetworkWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf,
				new Duration(1000));
	}
}
