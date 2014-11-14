package iie.hadoop.operator.spark.interfaces;

import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.Pair;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 
 * Spark转换算子。所有的Spark转换算子都应该实现该接口。
 * 
 * @author weixing
 *
 */
public interface TransformOp {
	/**
	 * 
	 * @param jsc
	 * @param pairs
	 * @return
	 */
	Pair<HCatSchema, JavaRDD<HCatRecord>> transform(JavaSparkContext jsc,
			Pair<HCatSchema, JavaRDD<HCatRecord>>... pairs);
}
