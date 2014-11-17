package iie.operator.api.streaming;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * 
 * Spark Streaming加载算子基类。所有的Spark Streaming加载算子都应该继承该基类。
 * 
 * @author weixing
 *
 */
public abstract class LoadOp implements Serializable {
	private static final long serialVersionUID = 6201901563623569626L;

	public LoadOp(String name) {

	}

	/**
	 * 
	 * @param jsc
	 * @param conf
	 *            用户获取用户配置的参数，键值对方式，可参照Hadoop中Configure。
	 *            在使用中，一般用于传递用户配置的目标表的模式及其表、目录等。
	 * @return 返回指定表或目录的数据，其中包含schema。
	 */
	public abstract DStreamWithSchema load(JavaStreamingContext jssc,
			Configuration conf);
}
