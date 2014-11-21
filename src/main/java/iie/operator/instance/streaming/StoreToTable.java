package iie.operator.instance.streaming;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.spark.SerializableWritable;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;
import iie.operator.api.format.SerHCatOutputFormat;
import iie.operator.api.streaming.DStreamWithSchema;
import iie.operator.api.streaming.StoreOp;

public class StoreToTable extends StoreOp {
	private static final long serialVersionUID = -2935046443983163196L;
	public static final String DATABASE_NAME = "database.name";
	public static final String TABLE_NAME = "table.name";

	public void store(JavaStreamingContext jssc, Configuration conf,
			DStreamWithSchema dstream) {
		String dbName = conf.get(DATABASE_NAME,
				MetaStoreUtils.DEFAULT_DATABASE_NAME);
		String tblName = conf.get(TABLE_NAME);
		Job outputJob = null;
		try {
			String[] resources = { "offline-core-site.xml",
					"offline-hdfs-site.xml", "offline-hive-site.xml",
					"offline-mapred-site.xml", "offline-ssl-client.xml",
					"offline-yarn-site.xml" };
			for (String resource : resources) {
				conf.addResource(resource);
			}
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
		dstream.getDStream()
				.mapToPair(
						new PairFunction<SerializableWritable<HCatRecord>, NullWritable, SerializableWritable<HCatRecord>>() {
							private static final long serialVersionUID = 1741555917449626517L;

							@Override
							public Tuple2<NullWritable, SerializableWritable<HCatRecord>> call(
									SerializableWritable<HCatRecord> record)
									throws Exception {
								return new Tuple2<NullWritable, SerializableWritable<HCatRecord>>(
										NullWritable.get(), record);
							}

						})
				.saveAsNewAPIHadoopFiles("", "", WritableComparable.class,
						SerializableWritable.class, SerHCatOutputFormat.class,
						outputJob.getConfiguration());
	}
}
