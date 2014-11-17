package iie.operator.api.spark;

import java.io.Serializable;

import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.spark.SerializableWritable;
import org.apache.spark.api.java.JavaRDD;

public class RDDWithSchema implements Serializable {
	private static final long serialVersionUID = -3323090535716961174L;

	private final String name;
	private final HCatSchema schema;
	private final JavaRDD<SerializableWritable<HCatRecord>> records;

	public RDDWithSchema(String name, HCatSchema schema,
			JavaRDD<SerializableWritable<HCatRecord>> records) {
		this.name = name;
		this.schema = schema;
		this.records = records;
	}

	public String getName() {
		return name;
	}

	public HCatSchema getSchema() {
		return schema;
	}

	public JavaRDD<SerializableWritable<HCatRecord>> getRecords() {
		return records;
	}

}
