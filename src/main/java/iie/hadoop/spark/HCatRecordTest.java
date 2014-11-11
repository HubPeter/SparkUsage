package iie.hadoop.spark;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.ReaderWriter;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import com.google.common.collect.Lists;

import scala.Tuple2;

public class HCatRecordTest {
	public static class HCatRecord1 extends HCatRecord implements Serializable {
		private static final long serialVersionUID = -5006683637103805505L;

		private List<Object> contents;

		public HCatRecord1() {
			contents = new ArrayList<Object>();
		}

		public HCatRecord1(int size) {
			contents = new ArrayList<Object>(size);
			for (int i = 0; i < size; i++) {
				contents.add(null);
			}
		}

		@Override
		public void remove(int idx) throws HCatException {
			contents.remove(idx);
		}

		public HCatRecord1(List<Object> list) {
			contents = list;
		}

		@Override
		public Object get(int fieldNum) {
			return contents.get(fieldNum);
		}

		@Override
		public List<Object> getAll() {
			return contents;
		}

		@Override
		public void set(int fieldNum, Object val) {
			contents.set(fieldNum, val);
		}

		@Override
		public int size() {
			return contents.size();
		}

		@Override
		public void readFields(DataInput in) throws IOException {

			contents.clear();
			int len = in.readInt();
			for (int i = 0; i < len; i++) {
				contents.add(ReaderWriter.readDatum(in));
			}
		}

		@Override
		public void write(DataOutput out) throws IOException {
			int sz = size();
			out.writeInt(sz);
			for (int i = 0; i < sz; i++) {
				ReaderWriter.writeDatum(out, contents.get(i));
			}

		}

		@Override
		public int hashCode() {
			int hash = 1;
			for (Object o : contents) {
				if (o != null) {
					hash = 31 * hash + o.hashCode();
				}
			}
			return hash;
		}

		@Override
		public String toString() {

			StringBuilder sb = new StringBuilder();
			for (Object o : contents) {
				sb.append(o + "\t");
			}
			return sb.toString();
		}

		@Override
		public Object get(String fieldName, HCatSchema recordSchema)
				throws HCatException {
			return get(recordSchema.getPosition(fieldName));
		}

		@Override
		public void set(String fieldName, HCatSchema recordSchema, Object value)
				throws HCatException {
			set(recordSchema.getPosition(fieldName), value);
		}

		@Override
		public void copy(HCatRecord r) throws HCatException {
			this.contents = r.getAll();
		}

	}

	public static void main(String[] args) {
		JavaSparkContext jsc = new JavaSparkContext(new SparkConf()
		// .set(
		// "spark.closure.serializer",
		// "org.apache.spark.serializer.KryoSerializer")
		);
		// List<Integer> list = new ArrayList<Integer>(3);
		// list.add(1);
		// list.add(2);
		// list.add(3);
		// JavaRDD<Integer> rdd = jsc.parallelize(list)
		// .map(new Function<Integer, HCatRecord>() {
		// private static final long serialVersionUID = -359000006677210042L;
		//
		// @Override
		// public HCatRecord call(Integer i) throws Exception {
		// HCatRecord record = new DefaultHCatRecord(1);
		// record.set(0, i);
		// return record;
		// }
		//
		// }).map(new Function<HCatRecord, Integer>() {
		// private static final long serialVersionUID = 3144966552984921176L;
		//
		// @Override
		// public Integer call(HCatRecord record) throws Exception {
		// return (Integer) record.get(0);
		// }
		//
		// });
		// Configuration inputConf = new Configuration();
		// try {
		// HCatInputFormat.setInput(inputConf, "wx", "tbl_spark_in");
		// } catch (IOException e) {
		// e.printStackTrace();
		// return;
		// }
		Job inputJob = null;
		try {
			inputJob = new Job(new Configuration(), "input");
			HCatInputFormat.setInput(inputJob, "wx", "tbl_spark_in");
		} catch (IOException e1) {
			e1.printStackTrace();
			return;
		}

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
					OutputJobInfo.create("wx", "tbl_spark_out", null));
			HCatSchema schema = HCatOutputFormat.getTableSchema(outputJob
					.getConfiguration());
			HCatOutputFormat.setSchema(outputJob, schema);
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		JavaRDD<HCatRecord> rdd = jsc
				.newAPIHadoopRDD(inputJob.getConfiguration(),
						HCatInputFormat.class, WritableComparable.class,
						HCatRecord.class)
				.map(new Function<Tuple2<WritableComparable, HCatRecord>, HCatRecord>() {

					/**
		 *
		 */
					private static final long serialVersionUID = -1326020671419059522L;

					@Override
					public HCatRecord call(
							Tuple2<WritableComparable, HCatRecord> v1)
							throws Exception {
						HCatRecord v2 = new HCatRecord1(3);
						for (int i = 0; i < v1._2.size(); ++i) {
							v2.set(i, v1._2.get(i));
						}
						v2.set(2,
								Lists.newArrayList(v1._2.get(1).toString()
										.split(" ")));
						return v2;
					}

				});
		rdd.persist(StorageLevel.DISK_ONLY());
		rdd.mapToPair(new PairFunction<HCatRecord, NullWritable, HCatRecord>() {

			/**
		 *
		 */
			private static final long serialVersionUID = 7497785706084425994L;

			@Override
			public Tuple2<NullWritable, HCatRecord> call(HCatRecord t)
					throws Exception {
				return new Tuple2<NullWritable, HCatRecord>(NullWritable.get(),
						t);
			}

		}).saveAsNewAPIHadoopDataset(outputJob.getConfiguration());
		System.out.println(rdd.count());

		jsc.stop();
	}
}
