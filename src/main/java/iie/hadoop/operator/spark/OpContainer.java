package iie.hadoop.operator.spark;

import iie.hadoop.operator.spark.instance.LoadFromTable;
import iie.hadoop.operator.spark.instance.StoreToTable;
import iie.hadoop.operator.spark.instance.Token;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

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
		Token transformOp = new Token();
		StoreToTable storeOp = new StoreToTable();

		// 算子进行串联执行。
		// TODO: 根据程序参数，为每个算子设置不同配置。
		Configuration conf1 = new Configuration();
		conf1.set(LoadFromTable.DATABASE_NAME, "wx");
		conf1.set(LoadFromTable.TABLE_NAME, "tbl_spark_in");
		loadOp.setConf(conf1);
		Configuration conf2 = new Configuration();
		conf2.set(Token.TOKEN_COLUMNS, "col2");
		transformOp.setConf(conf2);
		Configuration conf3 = new Configuration();
		conf3.set(StoreToTable.DATABASE_NAME, "wx");
		conf3.set(StoreToTable.TABLE_NAME, "tbl_spark_out");
		storeOp.setConf(conf3);

		storeOp.store(jsc, transformOp.transform(jsc, loadOp.load(jsc)));
		jsc.stop();

	}

}
