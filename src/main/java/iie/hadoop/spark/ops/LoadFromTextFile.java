package iie.hadoop.spark.ops;

import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class LoadFromTextFile implements LoadFromFileOp {

	@Override
	public JavaRDD<HCatRecord> fromFile(JavaSparkContext jsc, String path) {
		return jsc.textFile(path).map(new Function<String, HCatRecord>() {
			private static final long serialVersionUID = 9056678399516177605L;
			@Override
			public HCatRecord call(String line) throws Exception {
				HCatRecord record = new DefaultHCatRecord();

				String[] fields = line.split(",", -1);
				for (int i = 0; i < fields.length; ++i) {
					record.set(i, fields[i]);
				}

				return record;
			}

		});
	}

}
