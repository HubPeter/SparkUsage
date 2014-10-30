package iie.hadoop.spark;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.scheduler.SplitInfo;

import scala.Tuple2;
import scala.collection.Map;
import scala.collection.Set;

public final class WordCount {
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: WordCount <in> <out>");
			System.exit(1);
		}
		Map<String, Set<SplitInfo>> map = null;
		SparkContext ctxx = new SparkContext(new SparkConf(), map);
		String inputPath = args[0] + "/*";
		String outputPath = args[1];
		SparkConf sparkConf = new SparkConf().setAppName("word count");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = ctx.textFile(inputPath, 1);
		JavaRDD<String> words = lines
				.flatMap(new FlatMapFunction<String, String>() {
					@Override
					public Iterable<String> call(String s) {
						return Arrays.asList(SPACE.split(s));
					}
				});
		JavaPairRDD<String, Integer> ones = words
				.mapToPair(new PairFunction<String, String, Integer>() {
					@Override
					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<String, Integer>(s, 1);
					}
				});
		JavaPairRDD<String, Integer> counts = ones
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer i1, Integer i2) {
						return i1 + i2;
					}
				});
		counts.saveAsTextFile(outputPath);
		ctx.stop();
	}
}