package iie.operator.instance.spark;

import java.util.List;

import iie.operator.api.spark.LoadOp;
import iie.operator.api.spark.RDDWithSchema;
import iie.operator.api.spark.StoreOp;
import iie.operator.api.spark.TransformOp;

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
		Configuration conf1 = new Configuration();
		conf1.set(LoadFromTable.DATABASE_NAME, "wx");
		conf1.set(LoadFromTable.TABLE_NAME, "tbl_spark_in");
		Configuration conf2 = new Configuration();
		conf2.set(Token.TOKEN_COLUMNS, "col2");
		Configuration conf3 = new Configuration();
		conf3.set(StoreToTable.DATABASE_NAME, "wx");
		conf3.set(StoreToTable.TABLE_NAME, "tbl_spark_out");

		RDDWithSchema source = loadOp.load(jsc, conf1);
		List<RDDWithSchema> results = transformOp.transform(jsc, conf2,
				Lists.newArrayList(source));
		for (RDDWithSchema result : results) {
			storeOp.store(jsc, conf3, result);
		}
		jsc.stop();

	}

}
