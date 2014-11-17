package iie.hadoop.spark.streaming;

import iie.hadoop.spark.SparkUtils;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class SparkStreamingTest {
	public static void main(String[] args) {
		JavaStreamingContext jsc = new JavaStreamingContext(
				new SparkConf().setAppName("SparkStreamingTest"), new Duration(
						10000));
		Configuration conf = new Configuration();
		conf.set(StoreToTable.DATABASE_NAME, "wx");
		conf.set(StoreToTable.TABLE_NAME, "tbl_streaming_out");
		conf.set(LoadFromTextFile.DIRECTORY, "/tmp/streaming");
		conf.set(LoadFromTextFile.SCHEMA, "col1:int,col2:int,col3:int");
		LoadFromTextFile loadOp = new LoadFromTextFile();
		StoreToTable storeOp = new StoreToTable();
		StreamWithSchema stream = loadOp.load(jsc, conf);
		storeOp.store(stream, conf);
		jsc.start();
		jsc.awaitTermination();
	}

	public static class StreamWithSchema implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = -5264010313730316620L;
		public final HCatSchema schema;
		public final JavaDStream<HCatRecord> stream;

		public StreamWithSchema(HCatSchema schema,
				JavaDStream<HCatRecord> stream) {
			this.schema = schema;
			this.stream = stream;
		}
	}

	public static class LoadFromTextFile implements Serializable {
		private static final long serialVersionUID = -5718026961690960802L;
		public static final String SCHEMA = "schema";
		public static final String DIRECTORY = "directory";

		public StreamWithSchema load(JavaStreamingContext jsc,
				Configuration conf) {
			HCatSchema schema = getHCatSchema(conf);
			String directory = conf.get(DIRECTORY);
			JavaDStream<HCatRecord> stream = jsc.textFileStream(directory).map(
					new Function<String, HCatRecord>() {

						private static final long serialVersionUID = 2249039316572702298L;

						@Override
						public HCatRecord call(String line) throws Exception {
							line = "1 2 3";
							String[] fields = line.split(" ");
							DefaultHCatRecord record = new DefaultHCatRecord(
									fields.length);
							for (int i = 0; i < fields.length; ++i) {
								record.set(i, Integer.valueOf(fields[i]));
							}
							return record;
						}

					});
			return new StreamWithSchema(schema, stream);
		}

		public HCatSchema getHCatSchema(Configuration conf) {
			String[] strs = conf.get(SCHEMA).split(",");
			String[] fieldNames = new String[strs.length];
			String[] fieldTypes = new String[strs.length];
			for (int i = 0; i < strs.length; ++i) {
				String[] nameAndType = strs[i].split(":");
				fieldNames[i] = nameAndType[0];
				fieldTypes[i] = nameAndType[1];
			}

			try {
				return SparkUtils.getHCatSchema(fieldNames, fieldTypes);
			} catch (HCatException e) {
				e.printStackTrace();
			}
			return null;
		}
	}

	public static class StoreToTable implements Serializable {
		private static final long serialVersionUID = -2935046443983163196L;
		public static final String DATABASE_NAME = "database.name";
		public static final String TABLE_NAME = "table.name";

		public StoreToTable() {

		}

		public void store(StreamWithSchema stream, Configuration conf) {
			String dbName = conf.get(DATABASE_NAME,
					MetaStoreUtils.DEFAULT_DATABASE_NAME);
			String tblName = conf.get(TABLE_NAME);
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
			stream.stream.mapToPair(
					new PairFunction<HCatRecord, NullWritable, HCatRecord>() {
						private static final long serialVersionUID = 1741555917449626517L;

						@Override
						public Tuple2<NullWritable, HCatRecord> call(
								HCatRecord record) throws Exception {
							return new Tuple2<NullWritable, HCatRecord>(
									NullWritable.get(), record);
						}

					}).saveAsNewAPIHadoopFiles("", "", NullWritable.class,
					HCatRecord.class, HCatOutputFormat.class,
					outputJob.getConfiguration());
		}
	}

}
