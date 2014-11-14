package iie.hadoop.hcatalog;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

public class RecordWriterTest {

	public static void main(String[] args) {
		try {
		Job job = new Job();
		HCatOutputFormat.setOutput(job, OutputJobInfo.create("wx", "tbl_spark_in", null));
		HCatSchema schema = HCatOutputFormat.getTableSchema(job
				.getConfiguration());
		HCatOutputFormat.setSchema(job, schema);
		RecordWriter<WritableComparable<?>, HCatRecord> writer = new HCatOutputFormat()
				.getRecordWriter(new TaskAttemptContextImpl(job
						.getConfiguration(), TaskAttemptID
						.forName("attempt_200707121733_0003_m_000005_0")));
		List<Object> fields = new ArrayList<Object>(2);
		fields.add(1);
		fields.add("hello");
		writer.write(NullWritable.get(), new DefaultHCatRecord(fields));
		writer.close(null);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
