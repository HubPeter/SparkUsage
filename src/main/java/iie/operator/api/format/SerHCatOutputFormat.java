package iie.operator.api.format;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.spark.SerializableWritable;

public class SerHCatOutputFormat extends
		OutputFormat<WritableComparable, SerializableWritable> {

	private org.apache.hive.hcatalog.mapreduce.HCatOutputFormat instance = new org.apache.hive.hcatalog.mapreduce.HCatOutputFormat();

	@Override
	public RecordWriter<WritableComparable, SerializableWritable> getRecordWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		final RecordWriter<WritableComparable<?>, HCatRecord> writer = instance
				.getRecordWriter(context);
		return new RecordWriter<WritableComparable, SerializableWritable>() {

			@Override
			public void write(WritableComparable key, SerializableWritable value)
					throws IOException, InterruptedException {
				writer.write(key, (HCatRecord) value.value());
			}

			@Override
			public void close(TaskAttemptContext context) throws IOException,
					InterruptedException {
				writer.close(context);
			}

		};
	}

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException,
			InterruptedException {
		instance.checkOutputSpecs(context);
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		return instance.getOutputCommitter(context);
	}

	public static void setOutput(Job outputJob, OutputJobInfo outputJobInfo)
			throws IOException {
		org.apache.hive.hcatalog.mapreduce.HCatOutputFormat.setOutput(
				outputJob, outputJobInfo);
	}

	public static HCatSchema getTableSchema(Configuration conf)
			throws IOException {
		return org.apache.hive.hcatalog.mapreduce.HCatOutputFormat
				.getTableSchema(conf);
	}

	public static void setSchema(Job outputJob, HCatSchema schema)
			throws IOException {
		org.apache.hive.hcatalog.mapreduce.HCatOutputFormat.setSchema(
				outputJob, schema);
	}

}
