package iie.hadoop.operator.spark.instance;

import iie.hadoop.operator.spark.interfaces.LoadOp;
import iie.hadoop.operator.spark.interfaces.RDDWithSchema;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import com.google.common.collect.Lists;

/**
 * 
 * 实现的一个从表加载的Spark加载算子。
 * 
 * 算子会根据指定的数据库名和数据表名，将表中数据加载成rdd，并且生成对应的schema。
 * 
 * @author weixing
 *
 */
public class LoadFromTable implements LoadOp, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8892645107358023333L;
	public static final String DATABASE_NAME = "database.name";
	public static final String TABLE_NAME = "table.name";

	public LoadFromTable() {

	}

	@Override
	public List<String> getKeys() {
		return Lists.newArrayList(DATABASE_NAME, TABLE_NAME);
	}

	@Override
	public RDDWithSchema load(JavaSparkContext jsc, Configuration conf) {

		// 获取用户设置的要载入的表
		String dbName = conf.get(DATABASE_NAME);
		String tblName = conf.get(TABLE_NAME);
		HCatSchema schema = null;
		try {
			HCatInputFormat.setInput(conf, dbName, tblName);
			schema = HCatInputFormat.getTableSchema(conf);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

		// 构造HCatInputFormat，从对应的文件中读取数据，生成RDD
		JavaRDD<HCatRecord> rdd = jsc
				.newAPIHadoopRDD(conf, HCatInputFormat.class,
						WritableComparable.class, HCatRecord.class)
				.map(new Function<Tuple2<WritableComparable, HCatRecord>, HCatRecord>() {
					private static final long serialVersionUID = -2362812254158054659L;

					@Override
					public HCatRecord call(
							Tuple2<WritableComparable, HCatRecord> v)
							throws Exception {
						return v._2;
					}
				});
		return new RDDWithSchema(schema, rdd);
	}

}