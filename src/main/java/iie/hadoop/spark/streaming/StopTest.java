package iie.hadoop.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StopTest {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName(
				StopTest.class.getName());
		JavaStreamingContext jssc = new JavaStreamingContext(conf,
				new Duration(5000));
		JavaReceiverInputDStream<String> source = jssc.socketTextStream("localhost", 12345);
		source.print();
	}
}
