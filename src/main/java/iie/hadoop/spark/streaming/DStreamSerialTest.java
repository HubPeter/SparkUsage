package iie.hadoop.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class DStreamSerialTest {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName(
				"NetworkWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf,
				new Duration(1000));
		JavaReceiverInputDStream<String> source = jssc.socketTextStream(
				args[0], Integer.valueOf(args[1]));
		source.map(new Function<String, Integer>() {
			public Integer call(String v) {
				return v.length();
			}
		});
		source.transform(new Function<JavaRDD<String>, JavaRDD<Integer>>() {

			@Override
			public JavaRDD<Integer> call(JavaRDD<String> v) throws Exception {
				return v.map(new Function<String, Integer>() {

					@Override
					public Integer call(String v1) throws Exception {
						return v1.length();
					}
					
				});
			}
			
		}).print();
		jssc.awaitTermination();
	}
}
