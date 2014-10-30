package iie.hadoop.spark.graphx;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.graphx.Graph;

public class Test {

	public static void main(String[] args) {
		SparkContext sc = new SparkContext(new SparkConf().setAppName("Test"));
		Graph<String, Integer> graph = null;
		graph.edges();
	}

}
