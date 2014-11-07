package iie.hadoop.operator.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

public class MapRCopyOp extends Configured implements Tool {

	public static class Map extends
			Mapper<WritableComparable, HCatRecord, NullWritable, HCatRecord> {

		@Override
		protected void map(
				WritableComparable key,
				HCatRecord value,
				org.apache.hadoop.mapreduce.Mapper<WritableComparable, HCatRecord, NullWritable, HCatRecord>.Context context)
				throws IOException, InterruptedException {
			context.write(NullWritable.get(), value);
		}
	}

	public static class Reduce extends
			Reducer<NullWritable, HCatRecord, NullWritable, HCatRecord> {

		@Override
		protected void reduce(
				NullWritable key,
				java.lang.Iterable<HCatRecord> values,
				org.apache.hadoop.mapreduce.Reducer<NullWritable, HCatRecord, NullWritable, HCatRecord>.Context context)
				throws IOException, InterruptedException {
			for (HCatRecord value : values) {
				context.write(NullWritable.get(), value);
			}
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		args = new GenericOptionsParser(conf, args).getRemainingArgs();

		// 获取指定的要加载表和目标表的表名
		String databaseName = "default";
		String inputTableName = args[0];
		String outputTableName = args[1];

		Job job = new Job(conf, "MapRCopyOp");
		// 将要加载表信息添加到configuration中
		HCatInputFormat.setInput(job, databaseName, inputTableName);

		// 使用要加载表的schema，创建目标表
		HCatSchema schema = HCatInputFormat.getTableSchema(conf);
		HiveMetaStoreClient client = HCatUtil.getHiveClient(new HiveConf());
		Table table = new Table();
		table.setDbName(databaseName);
		table.setTableName(outputTableName);

		StorageDescriptor sd = new StorageDescriptor();
		List<FieldSchema> cols = HCatUtil
				.getFieldSchemaList(schema.getFields());
		sd.setCols(cols);
		table.setSd(sd);
		client.createTable(table);

		// 将目标表信息添加到configuration中
		HCatOutputFormat.setOutput(conf, null,
				OutputJobInfo.create(databaseName, outputTableName, null));
		HCatOutputFormat.setSchema(conf, schema);

		// 设置MapReduce相关信息
		job.setJarByClass(MapRCopyOp.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(HCatInputFormat.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(HCatRecord.class);
		job.setOutputFormatClass(HCatOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(HCatRecord.class);
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MapRCopyOp(), args);
		System.exit(exitCode);
	}
}