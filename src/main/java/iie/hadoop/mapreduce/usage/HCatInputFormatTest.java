package iie.hadoop.mapreduce.usage;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

public class HCatInputFormatTest extends Configured implements Tool {

	public static class Map extends
			Mapper<WritableComparable, HCatRecord, IntWritable, Text> {

		@Override
		protected void map(
				WritableComparable key,
				HCatRecord value,
				org.apache.hadoop.mapreduce.Mapper<WritableComparable, HCatRecord, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			context.write(new IntWritable((Integer)value.get(0)), new Text(value.get(1).toString()));
		}
	}

	public static class Reduce extends
			Reducer<IntWritable, Text, WritableComparable, DefaultHCatRecord> {

		@Override
		protected void reduce(
				IntWritable key,
				java.lang.Iterable<Text> values,
				org.apache.hadoop.mapreduce.Reducer<IntWritable, Text, WritableComparable, DefaultHCatRecord>.Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				DefaultHCatRecord record = new DefaultHCatRecord(2);
				record.set(0, key.get());
				record.set(1, value);
				context.write(NullWritable.get(), record);
			}
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.set("hive.metastore.uris", "thrift://m105:9083");
		args = new GenericOptionsParser(conf, args).getRemainingArgs();

		String inputTableName = args[0];
		String outputTableName = args[1];
		String dbName = "wx";

		Job job = new Job(conf, "HCatInputFormatTest");
		HCatInputFormat.setInput(job, dbName, inputTableName);

		job.setInputFormatClass(HCatInputFormat.class);
		job.setJarByClass(HCatInputFormatTest.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(WritableComparable.class);
		job.setOutputValueClass(DefaultHCatRecord.class);
		HCatOutputFormat.setOutput(job,
				OutputJobInfo.create(dbName, outputTableName, null));
		HCatSchema s = HCatOutputFormat.getTableSchema(job);
        System.err.println("INFO: output schema explicitly set for writing:"
                + s);
        HCatOutputFormat.setSchema(job, s);
		job.setOutputFormatClass(HCatOutputFormat.class);
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new HCatInputFormatTest(), args);
		System.exit(exitCode);
	}
}