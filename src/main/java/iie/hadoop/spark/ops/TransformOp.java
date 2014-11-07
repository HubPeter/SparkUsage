package iie.hadoop.spark.ops;

import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.spark.api.java.JavaRDD;

public interface TransformOp {

	public static final JavaRDD<HCatRecord> ERROR = new JavaRDD<HCatRecord>(
			null, null);

	public JavaRDD<HCatRecord> transform(JavaRDD<HCatRecord> rdd);

}
