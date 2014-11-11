package iie.hadoop.operator.spark;

import iie.hadoop.operator.spark.instance.LoadFromTable;
import iie.hadoop.operator.spark.instance.StoreToTable;
import iie.hadoop.operator.spark.instance.Token;
import iie.hadoop.operator.spark.interfaces.LoadOp;
import iie.hadoop.operator.spark.interfaces.RDDWithSchema;
import iie.hadoop.operator.spark.interfaces.StoreOp;
import iie.hadoop.operator.spark.interfaces.TransformOp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Lists;
import com.sun.tools.internal.jxc.gen.config.Config;

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
		LoadFromTable loadOp = new LoadFromTable();
		TransformOp transformOp = new Token();
		StoreOp storeOp = new StoreToTable();

		// 算子进行串联执行。
		// TODO: 根据程序参数，为每个算子设置不同配置。
		Configuration conf1 = new Configuration();
		conf1.set(LoadFromTable.DATABASE_NAME, "wx");
		conf1.set(LoadFromTable.TABLE_NAME, "tbl_spark_in");
		RDDWithSchema records = loadOp.load(jsc, conf1);
		System.out.println(records.rdd.count());
		Configuration conf2 = new Configuration();
		conf2.set(Token.TOKEN_COLUMNS, "col2");
		records = transformOp
				.transform(jsc, conf2, Lists.newArrayList(records));
		Configuration conf3 = new Configuration();
		conf3.set(StoreToTable.DATABASE_NAME, "wx");
		conf3.set(StoreToTable.TABLE_NAME, "tbl_spark_out");
		storeOp.store(jsc, conf3, records);

		jsc.stop();

	}

}
