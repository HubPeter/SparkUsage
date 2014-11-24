package iie.operator.api.spark.text;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 
 * Spark存储算子基类。所有的Spark存储算子都应该继承该基类。
 *
 * @author weixing
 *
 */
public abstract class StoreOp implements Serializable {
	private static final long serialVersionUID = 4203348313448761469L;

	/**
	 * @param jsc
	 * @param conf
	 *            用户获取用户配置的参数，键值对方式，可参照Hadoop中Configure。
	 *            在使用中，一般用于传递用户配置的目标表的模式及其表、目录等。
	 * @param rdd
	 *            要保存的RDD
	 */
	public abstract void store(JavaSparkContext jsc, Configuration conf,
			JavaRDD<String> rdd);
}
