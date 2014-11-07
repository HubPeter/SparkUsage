package iie.hadoop.operator.spark.interfaces;

import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.spark.api.java.JavaRDD;

/**
 * 
 * Spark算子之间传递的数据类型，包含以下兩部分：
 * 
 * rdd: 以HCatRecord{@link HCatRecord}为元素的JavaRDD{@link JavaRDD}；
 * 
 * schema：描述 HCatRecord{@link HCatRecord}各字段类型的HCatSchema{@link HCatSchema}。
 * 
 * @author weixing
 *
 */
public class RDDWithSchema {
	public final HCatSchema schema;
	public final JavaRDD<HCatRecord> rdd;

	public RDDWithSchema(HCatSchema schema, JavaRDD<HCatRecord> rdd) {
		this.schema = schema;
		this.rdd = rdd;
	}
}
