package iie.operator.api.streaming;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * 
 * Spark Streaming转换算子基类。所有的Spark Streaming转换算子都应该继承该基类。
 * 
 * @author weixing
 *
 */
public abstract class TransformOp implements Serializable {
	private static final long serialVersionUID = 3655484892882276916L;

	/**
	 * 
	 * @param jssc
	 * @param conf 配置选项
	 * @param dstreams 需要转换的多个DStream
	 * @return 转换后的DStream
	 */
	public abstract List<DStreamWithSchema> transform(
			JavaStreamingContext jssc, Configuration conf,
			List<DStreamWithSchema> dstreams);
}
