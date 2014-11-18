package iie.operator.instance.spark;

import iie.operator.api.format.SerHCatInputFormat;
import iie.operator.api.spark.LoadOp;
import iie.operator.api.spark.RDDWithSchema;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.spark.SerializableWritable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

/**
 * 
 * 实现的一个从表加载的Spark加载算子。
 * 
 * 算子会根据指定的数据库名和数据表名，将表中数据加载成rdd，并且生成对应的schema。
 * 
 * @author weixing
 *
 */
public class LoadFromTable extends LoadOp {
	private static final long serialVersionUID = 8892645107358023333L;
	public static final String DATABASE_NAME = "database.name";
	public static final String TABLE_NAME = "table.name";

	@Override
	public RDDWithSchema load(JavaSparkContext jsc, Configuration conf) {
		String dbName = conf.get(DATABASE_NAME,
				MetaStoreUtils.DEFAULT_DATABASE_NAME);
		String tblName = conf.get(TABLE_NAME);

		// 获取用户设置的要载入的表
		HCatSchema schema = null;
		try {
			SerHCatInputFormat.setInput(conf, dbName, tblName);
			schema = SerHCatInputFormat.getTableSchema(conf);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

		// 构造HCatInputFormat，从对应的文件中读取数据，生成RDD
		JavaRDD<SerializableWritable<HCatRecord>> rdd = jsc
				.newAPIHadoopRDD(conf, SerHCatInputFormat.class,
						WritableComparable.class, SerializableWritable.class)
				.map(new Function<Tuple2<WritableComparable, SerializableWritable>, SerializableWritable<HCatRecord>>() {
					private static final long serialVersionUID = -2362812254158054659L;

					@Override
					public SerializableWritable<HCatRecord> call(
							Tuple2<WritableComparable, SerializableWritable> v)
							throws Exception {
						return v._2;
					}
				});
		rdd.persist(StorageLevel.DISK_ONLY());
		return new RDDWithSchema("", schema, rdd);
	}

}
