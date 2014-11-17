package iie.operator.streaming.instance;

import iie.operator.api.streaming.DStreamWithSchema;
import iie.operator.api.streaming.LoadOp;
import iie.operator.api.streaming.StoreOp;
import iie.operator.api.streaming.TransformOp;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.collect.Lists;

/**
 * 
 * 实现的一个简单的算子串联执行的示例。该Spark Streaming算子容器仅为简单实例，由三个算子串联组成。
 * 
 * 程序实现的是数据从一个目录中的文件加载，并对指定列进行分词，最后保存到另外一个表。
 * 
 * 各算子所需的配置项可从程序参数中指定。
 * 
 * @author weixing
 *
 */
public class OpContainer {

	public static void main(String[] args) {

		JavaStreamingContext jssc = new JavaStreamingContext(
				new SparkConf().setAppName("SparkStreamingOperatorTest"),
				new Duration(10000));

		// 实例化三个算子。
		LoadOp loadOp = new LoadFromTextFile();
		TransformOp transformOp = new Token();
		StoreOp storeOp = new StoreToTable();

		// 算子进行串联执行。
		// TODO: 根据程序参数，为每个算子设置不同配置。
		Configuration conf1 = new Configuration();
		conf1.set(LoadFromTextFile.DIRECTORY, "/tmp/streaming");
		conf1.set(LoadFromTextFile.SCHEMA, "col1:int,col2:string");
		Configuration conf2 = new Configuration();
		conf2.set(Token.TOKEN_COLUMNS, "col2");
		Configuration conf3 = new Configuration();
		conf3.set(StoreToTable.DATABASE_NAME, "wx");
		conf3.set(StoreToTable.TABLE_NAME, "tbl_spark_out");

		DStreamWithSchema source = loadOp.load(jssc, conf1);
		List<DStreamWithSchema> results = transformOp.transform(jssc, conf2,
				Lists.newArrayList(source));
		for (DStreamWithSchema result : results) {
			storeOp.store(jssc, conf3, result);
		}
		jssc.start();
		jssc.awaitTermination();
	}

}
