package iie.hadoop.spark.ops;

import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import com.google.common.collect.Lists;

public class TokenOp implements TransformOp {

	private String cols = "col2";
	private String schema = "col1:int,col2:string";

	@Override
	public JavaRDD<HCatRecord> transform(JavaRDD<HCatRecord> rdd) {
		final HCatSchema recordSchema = null;
		return rdd.map(new Function<HCatRecord, HCatRecord>() {
			private static final long serialVersionUID = 1300834901987070295L;

			@Override
			public HCatRecord call(HCatRecord record) throws Exception {
				String[] columns = cols.split(",");
				DefaultHCatRecord result = new DefaultHCatRecord(record.size() + columns.length);
				int i = 0;
				for (; i < record.getAll().size(); ++i) {
					result.set(i, record.get(i));
				}
				for (String column : columns) {
					String[] tokens = record.getString(column, recordSchema)
							.split(" ");
					result.set(i++, Lists.newArrayList(tokens));
				}
				return result;
			}

		});
	}

}
