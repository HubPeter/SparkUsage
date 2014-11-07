package iie.hadoop.operator.spark;

import iie.hadoop.operator.spark.instance.LoadFromTable;
import iie.hadoop.operator.spark.instance.StoreToTable;
import iie.hadoop.operator.spark.instance.Token;
import iie.hadoop.operator.spark.interfaces.LoadOp;
import iie.hadoop.operator.spark.interfaces.RDDWithSchema;
import iie.hadoop.operator.spark.interfaces.StoreOp;
import iie.hadoop.operator.spark.interfaces.TransformOp;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Lists;

/**
 * 
 * 实现的一个简单的算子串联执行的示例。该spark算子容器仅为简单实例，由三个算子串联组成。
 * 
 * 程序实现的是数据从一个表中加载，并对指定列进行分词，最后保存到另外一个表。
 * 
 * 各算子所需的配置项可从程序参数中指定。
 * 
 * @author weixing
 *
 */
public class OpContainer {

	public static void main(String[] args) {

		JavaSparkContext jsc = new JavaSparkContext(new SparkConf());

		// 实例化三个算子。
		LoadOp loadOp = new LoadFromTable();
		TransformOp transformOp = new Token();
		StoreOp storeOp = new StoreToTable();

		// 算子进行串联执行。
		// TODO: 根据程序参数，为每个算子设置不同配置。
		RDDWithSchema records = loadOp.load(jsc, new Configuration());
		records = transformOp.transform(jsc, new Configuration(),
				Lists.newArrayList(records));
		storeOp.store(jsc, new Configuration(), records);

		jsc.stop();

	}

}
