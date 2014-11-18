package iie.operator.api.spark;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 
 * Spark加载算子基类。所有的Spark加载算子都应该继承该基类。
 * 
 * @author weixing
 *
 */
public abstract class LoadOp implements Serializable {
	private static final long serialVersionUID = -4810093850347548583L;

	/**
	 * 
	 * @param jsc
	 * @param conf
	 *            用户获取用户配置的参数，键值对方式，可参照Hadoop中Configure。
	 *            在使用中，一般用于传递用户配置的目标表的模式及其表、目录等。
	 * @return 返回指定表或目录的数据，其中包含schema。
	 */
	public abstract RDDWithSchema load(JavaSparkContext jsc, Configuration conf);
}
