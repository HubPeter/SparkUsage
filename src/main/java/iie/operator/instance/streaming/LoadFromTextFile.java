package iie.operator.instance.streaming;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.spark.SerializableWritable;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import iie.operator.api.streaming.DStreamWithSchema;
import iie.operator.api.streaming.LoadOp;

public class LoadFromTextFile extends LoadOp {
	private static final long serialVersionUID = -1081941253643802506L;
	public static final String SCHEMA = "schema";
	public static final String DIRECTORY = "directory";

	public LoadFromTextFile() {
		super(null);
	}

	public DStreamWithSchema load(JavaStreamingContext jssc, Configuration conf) {
		HCatSchema schema = null;
		try {
			schema = getHCatSchema(conf);
		} catch (HCatException e) {
			e.printStackTrace();
			return null;
		}
		String directory = conf.get(DIRECTORY);
		JavaDStream<SerializableWritable<HCatRecord>> dstream = jssc
				.textFileStream(directory)
				.map(new Function<String, SerializableWritable<HCatRecord>>() {

					private static final long serialVersionUID = 2249039316572702298L;

					@Override
					public SerializableWritable<HCatRecord> call(String line)
							throws Exception {
						String[] fields = line.split("\t");
						DefaultHCatRecord record = new DefaultHCatRecord(
								fields.length);
						record.set(0, Integer.valueOf(fields[0]));
						record.set(1, fields[1]);
						return new SerializableWritable<HCatRecord>(record);
					}

				});
		return new DStreamWithSchema("", schema, dstream);
	}

	public static HCatSchema getHCatSchema(Configuration conf)
			throws HCatException {
		String[] strs = conf.get(SCHEMA).split(",");
		String[] fieldNames = new String[strs.length];
		String[] fieldTypes = new String[strs.length];
		for (int i = 0; i < strs.length; ++i) {
			String[] nameAndType = strs[i].split(":");
			fieldNames[i] = nameAndType[0];
			fieldTypes[i] = nameAndType[1];
		}
		List<HCatFieldSchema> fieldSchemas = new ArrayList<HCatFieldSchema>(
				fieldNames.length);

		for (int i = 0; i < fieldNames.length; ++i) {
			HCatFieldSchema.Type type = HCatFieldSchema.Type
					.valueOf(fieldTypes[i].toUpperCase());
			fieldSchemas.add(new HCatFieldSchema(fieldNames[i], type, ""));
		}
		return new HCatSchema(fieldSchemas);
	}

}