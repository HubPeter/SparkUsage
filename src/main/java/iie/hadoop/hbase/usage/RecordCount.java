package iie.hadoop.hbase.usage;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

public class RecordCount {

	public static class MyMapper extends TableMapper<Text, LongWritable> {
		public void map(ImmutableBytesWritable row, Result value,
				Context context) throws InterruptedException, IOException {
			context.write(new Text("test"), new LongWritable(1));
		}
	}

	public static class MyTableReducer extends
			TableReducer<Text, IntWritable, ImmutableBytesWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int i = 0;
			for (IntWritable val : values) {
				i += val.get();
			}
			Put put = new Put(Bytes.toBytes(key.toString()));
			put.add(Bytes.toBytes("cf"), Bytes.toBytes("count"),
					Bytes.toBytes(i));

			context.write(null, put);
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration config = HBaseConfiguration.create();
		Job job = new Job(config, "ExampleReadWrite");
		job.setJarByClass(RecordCount.class); // class that contains mapper

		Scan scan = new Scan();
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for
								// MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs
		// set other scan attrs

		TableMapReduceUtil.initTableMapperJob("test", // input table
				scan, // Scan instance to control CF and attribute selection
				MyMapper.class, // mapper class
				null, // mapper output key
				null, // mapper output value
				job);
		TableMapReduceUtil.initTableReducerJob("test3", // output table
				null, // reducer class
				job);
		job.setNumReduceTasks(0);

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}

}
