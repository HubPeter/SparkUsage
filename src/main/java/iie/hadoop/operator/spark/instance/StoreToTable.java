package iie.hadoop.operator.spark.instance;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
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
public class StoreToTable implements StoreOp, Serializable {
	private static final long serialVersionUID = -7869241353265241365L;

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
		// TODO: 根据用户设置的表名，创建目标表

		Job outputJob = null;
		try {
			outputJob = new Job(new Configuration(), "output");
			outputJob.setOutputFormatClass(HCatOutputFormat.class);
			outputJob.setOutputKeyClass(NullWritable.class);
			outputJob.setOutputValueClass(HCatRecord.class);
			outputJob.getConfiguration().set("mapreduce.task.attempt.id",
					"attempt__0000_r_000000_0");
			outputJob.getConfiguration().set("mapred.task.partition", "2");
			HCatOutputFormat.setOutput(outputJob,
					OutputJobInfo.create(dbName, tblName, null));
			HCatSchema schema = HCatOutputFormat.getTableSchema(outputJob
					.getConfiguration());
			HCatOutputFormat.setSchema(outputJob, schema);
		} catch (IOException e) {
			e.printStackTrace();
			return;
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
				}).saveAsNewAPIHadoopDataset(outputJob.getConfiguration());
	}

	public void createTable() {
		// TODO: 完成创建表过程
		// HiveMetaStoreClient client = HCatUtil.getHiveClient(new HiveConf());
		// Table table = new Table();
		// table.setDbName(dbName);
		// table.setTableName(tblName);
		//
		// StorageDescriptor sd = new StorageDescriptor();
		// List<FieldSchema> cols = HCatUtil.getFieldSchemaList(rdd.schema
		// .getFields());
		// sd.setCols(cols);
		// table.setSd(sd);
		// client.createTable(table);
	}
}
