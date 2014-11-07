package iie.hadoop.operator.spark.interfaces;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 
 * Spark存储算子接口。所有的Spark存储算子都应该实现该接口。
 *
 * @author weixing
 *
 */
public interface StoreOp extends Configurable {
	/**
	 * @param jsc
	 * @param conf
	 *            用户获取用户配置的参数，键值对方式，可参照Hadoop中Configure。 
	 *            在使用中，一般用于传递用户配置的目标表的模式及其表、目录等。
	 * @param rdd
	 *            要保存的RDD及其schema描述
	 */
	void store(JavaSparkContext jsc, Configuration conf, RDDWithSchema rdd);
}