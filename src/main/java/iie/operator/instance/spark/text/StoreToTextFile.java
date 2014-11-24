package iie.operator.instance.spark.text;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import iie.operator.api.spark.text.StoreOp;

public class StoreToTextFile extends StoreOp {
	private static final long serialVersionUID = -5794582464838866152L;
	public static final String OUTPUT_PATH = "output.path";

	/**
	 * 和使用HCatalog的存储算子不同的是，
	 * 文本存储算子只能将String类型的RDD保存到一个指定的路径下。
	 */
	@Override
	public void store(JavaSparkContext jsc, Configuration conf,
			JavaRDD<String> rdd) {
		String outputPath = conf.get(OUTPUT_PATH);
		rdd.saveAsTextFile(outputPath);
	}

}
