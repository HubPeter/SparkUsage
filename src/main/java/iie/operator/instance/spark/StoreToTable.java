package iie.operator.instance.spark;

import iie.operator.api.format.SerHCatOutputFormat;
import iie.operator.api.spark.RDDWithSchema;
import iie.operator.api.spark.StoreOp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.spark.SerializableWritable;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * 
 * 实现的一个存储到表的Spark存储算子。
 * 
 * 算子会根据指定的数据库名、数据表名以及schema，创建一个新表，然后将数据存储到该表。
 * 
 * @author weixing
 *
 */
public class StoreToTable extends StoreOp {
	private static final long serialVersionUID = -7429366527563875869L;
	public static final String DATABASE_NAME = "database.name";
	public static final String TABLE_NAME = "table.name";

	@Override
	public void store(JavaSparkContext jsc, Configuration conf,
			RDDWithSchema rdd) {
		String dbName = conf.get(DATABASE_NAME,
				MetaStoreUtils.DEFAULT_DATABASE_NAME);
		String tblName = conf.get(TABLE_NAME);
		Job outputJob = null;
		try {
			outputJob = new Job(conf, "output");
			outputJob.setOutputFormatClass(SerHCatOutputFormat.class);
			outputJob.setOutputKeyClass(WritableComparable.class);
			outputJob.setOutputValueClass(SerializableWritable.class);
			outputJob.getConfiguration().set("mapreduce.task.attempt.id",
					"attempt__0000_r_000000_0");
			outputJob.getConfiguration().set("mapred.task.partition", "2");
			SerHCatOutputFormat.setOutput(outputJob,
					OutputJobInfo.create(dbName, tblName, null));
			HCatSchema schema = SerHCatOutputFormat.getTableSchema(outputJob
					.getConfiguration());
			SerHCatOutputFormat.setSchema(outputJob, schema);
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		// 将RDD存储到目标表中
		rdd.getRecords()
				.mapToPair(
						new PairFunction<SerializableWritable<HCatRecord>, NullWritable, SerializableWritable<HCatRecord>>() {
							private static final long serialVersionUID = -4658431554556766962L;

							@Override
							public Tuple2<NullWritable, SerializableWritable<HCatRecord>> call(
									SerializableWritable<HCatRecord> record)
									throws Exception {
								return new Tuple2<NullWritable, SerializableWritable<HCatRecord>>(
										NullWritable.get(), record);
							}
						})
				.saveAsNewAPIHadoopDataset(outputJob.getConfiguration());
	}

}
