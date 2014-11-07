package iie.hadoop.spark.ops;

import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public interface LoadFromFileOp {

	JavaRDD<HCatRecord> fromFile(JavaSparkContext jsc, String path);

}
