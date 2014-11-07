package iie.hadoop.spark;

import java.io.IOException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.StructType;

public class TextFileToTable {

	public static void main(String[] args) throws IOException {
		JavaSparkContext jsc = new JavaSparkContext(
				new SparkConf().setAppName("TextFileToTable"));

		String[] fieldNames = { "col1", "col2" };
		String[] fieldTypes = { "int", "string" };
		StructType schema = SparkUtils.getStructType(fieldNames, fieldTypes);

		JavaRDD<HCatRecord> rdd1 = SparkUtils.readFromTextFile(jsc,
				"/path/to/file");

		JavaRDD<HCatRecord> rdd2 = SparkUtils.filter(rdd1);

		JavaRDD<HCatRecord> rdd3 = SparkUtils.sparkSQL(jsc, rdd2, schema,
				"test", "select * from test");

		SparkUtils.saveToTable(rdd3, "db", "tbl");
		jsc.stop();
	}

}
