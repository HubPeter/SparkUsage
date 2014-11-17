package iie.operator.api.streaming;

import java.io.Serializable;

import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.spark.SerializableWritable;
import org.apache.spark.streaming.api.java.JavaDStream;

public class DStreamWithSchema implements Serializable {
	private static final long serialVersionUID = 8350171472725712443L;

	private final String name;
	private final HCatSchema schema;
	private final JavaDStream<SerializableWritable<HCatRecord>> dstream;

	public DStreamWithSchema(String name, HCatSchema schema,
			JavaDStream<SerializableWritable<HCatRecord>> dstream) {
		this.name = name;
		this.schema = schema;
		this.dstream = dstream;
	}

	public String getName() {
		return name;
	}

	public HCatSchema getSchema() {
		return schema;
	}

	public JavaDStream<SerializableWritable<HCatRecord>> getDStream() {
		return dstream;
	}

}
