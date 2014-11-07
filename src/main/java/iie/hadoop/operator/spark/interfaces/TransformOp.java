package iie.hadoop.operator.spark.interfaces;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 
 * Spark转换算子。所有的Spark转换算子都应该实现该接口。
 * 
 * @author weixing
 *
 */
public interface TransformOp extends Configurable {
	/**
	 * 
	 * @param jsc
	 * @param conf
	 * @param rdds
	 * @return
	 */
	RDDWithSchema transform(JavaSparkContext jsc, Configuration conf,
			List<RDDWithSchema> rdds);
}