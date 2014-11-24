package iie.operator.instance.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TextFileWordCount extends Configured implements Tool {

	public static class Token extends
			Mapper<LongWritable, Text, Text, LongWritable> {
		private static final LongWritable one = new LongWritable(1L);

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			String col = value.toString().split("\t")[0];
			for (String token : col.split(" ")) {
				context.write(new Text(token), one);
			}
		}
	}

	public static class Sum extends
			Reducer<Text, LongWritable, LongWritable, Text> {
		private static final LongWritable zero = new LongWritable(0L);

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			long sum = 0L;
			for (LongWritable value : values) {
				sum += value.get();
			}
			context.write(zero, new Text(key.toString() + "\t" + sum));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		Job job = new Job(conf, getClass().getName());
		job.setJarByClass(getClass());
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MapRCopyOp(), args);
		System.exit(exitCode);
	}

}
