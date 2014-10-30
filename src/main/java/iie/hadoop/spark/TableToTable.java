package iie.hadoop.spark;

import java.io.IOException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TableToTable {

	public static void main(String[] args) throws IOException {
		JavaSparkContext jsc = new JavaSparkContext(
				new SparkConf().setAppName("HCatInputFormatTest"));

		JavaRDD<HCatRecord> rdd1 = SparkUtils.readFromTable(jsc, "", "");
		JavaRDD<HCatRecord> rdd2 = SparkUtils.filter(rdd1);
		SparkUtils.saveToTable(rdd2, "", "");
		jsc.stop();
	}


}
