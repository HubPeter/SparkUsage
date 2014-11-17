package iie.operator.api.format;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.spark.SerializableWritable;

public class SerHCatInputFormat extends
		InputFormat<WritableComparable, SerializableWritable> {
	private org.apache.hive.hcatalog.mapreduce.HCatInputFormat instance = new org.apache.hive.hcatalog.mapreduce.HCatInputFormat();

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		return instance.getSplits(context);
	}

	@Override
	public RecordReader<WritableComparable, SerializableWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		final RecordReader<WritableComparable, HCatRecord> reader = instance
				.createRecordReader(split, context);
		return new RecordReader<WritableComparable, SerializableWritable>() {

			@Override
			public void initialize(InputSplit split, TaskAttemptContext context)
					throws IOException, InterruptedException {
				reader.initialize(split, context);
			}

			@Override
			public boolean nextKeyValue() throws IOException,
					InterruptedException {
				return reader.nextKeyValue();
			}

			@Override
			public NullWritable getCurrentKey() throws IOException,
					InterruptedException {
				return NullWritable.get();
			}

			@Override
			public SerializableWritable<HCatRecord> getCurrentValue()
					throws IOException, InterruptedException {
				return new SerializableWritable<HCatRecord>(
						reader.getCurrentValue());
			}

			@Override
			public float getProgress() throws IOException, InterruptedException {
				return reader.getProgress();
			}

			@Override
			public void close() throws IOException {
				reader.close();
			}

		};
	}

	public static void setInput(Configuration conf, String dbName,
			String tblName) throws IOException {
		org.apache.hive.hcatalog.mapreduce.HCatInputFormat.setInput(conf,
				dbName, tblName);
	}

	public static HCatSchema getTableSchema(Configuration conf)
			throws IOException {
		return org.apache.hive.hcatalog.mapreduce.HCatInputFormat
				.getTableSchema(conf);
	}

}
