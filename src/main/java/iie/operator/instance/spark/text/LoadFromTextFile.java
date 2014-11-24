package iie.operator.instance.spark.text;

import iie.operator.api.spark.text.LoadOp;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 
 * 实现的一个从表加载的Spark加载算子。
 * 
 * @author weixing
 *
 */
public class LoadFromTextFile extends LoadOp {
	private static final long serialVersionUID = 8892645107358023333L;
	public static final String INPUT_PATH = "input.path";

	/**
	 * 与使用HCatalog的加载算子不同的是，
	 * 文本数据加载算子只能获取到一个文件或者一个路径下所有文件的数据。
	 */
	@Override
	public JavaRDD<String> load(JavaSparkContext jsc, Configuration conf) {
		String inputPath = conf.get(INPUT_PATH);
		if (inputPath == null) {
			return null;
		}
		return jsc.textFile(inputPath);
	}

}
