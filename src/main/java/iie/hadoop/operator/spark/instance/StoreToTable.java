package iie.hadoop.operator.spark.instance;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.io.NullWritable;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.thrift.TException;

import scala.Tuple2;

import com.google.common.collect.Lists;

import iie.hadoop.operator.spark.interfaces.RDDWithSchema;
import iie.hadoop.operator.spark.interfaces.StoreOp;

/**
 * 
 * 实现的一个存储到表的Spark存储算子。
 * 
 * 算子会根据指定的数据库名、数据表名以及schema，创建一个新表，然后将数据存储到该表。
 * 
 * @author weixing
 *
 */
public class StoreToTable implements StoreOp {

	public static final String DATABASE_NAME = "database.name";
	public static final String TABLE_NAME = "table.name";

	@Override
	public List<String> getKeys() {
		return Lists.newArrayList(DATABASE_NAME, TABLE_NAME);
	}

	@Override
	public void store(JavaSparkContext jsc, Configuration conf,
			RDDWithSchema rdd) {
		String dbName = conf.get(DATABASE_NAME,
				MetaStoreUtils.DEFAULT_DATABASE_NAME);
		String tblName = conf.get(TABLE_NAME);
		try {
			// 根据用户设置的表名，创建目标表
			HiveMetaStoreClient client = HCatUtil.getHiveClient(new HiveConf());
			Table table = new Table();
			table.setDbName(dbName);
			table.setTableName(tblName);

			StorageDescriptor sd = new StorageDescriptor();
			List<FieldSchema> cols = HCatUtil.getFieldSchemaList(rdd.schema
					.getFields());
			sd.setCols(cols);
			table.setSd(sd);
			client.createTable(table);
			HCatOutputFormat.setOutput(conf, null,
					OutputJobInfo.create(dbName, tblName, null));
		} catch (IOException | TException e) {
			e.printStackTrace();
		}
		
		// 将RDD存储到目标表中
		rdd.rdd.mapToPair(
				new PairFunction<HCatRecord, NullWritable, HCatRecord>() {
					private static final long serialVersionUID = -4658431554556766962L;

					@Override
					public Tuple2<NullWritable, HCatRecord> call(
							HCatRecord record) throws Exception {
						return new Tuple2<NullWritable, HCatRecord>(
								NullWritable.get(), record);
					}
				}).saveAsNewAPIHadoopDataset(conf);
	}
}
