package iie.hadoop.spark.ops;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class StoreToHive implements StoreToTableOp {

	@Override
	public void toTable(JavaRDD<HCatRecord> rdd, String dbName, String tblName) {
		Configuration outputConf = new Configuration();
		outputConf.set("hive.metastore.uris", "thrift://m105:9083");
		try {
			// TODO: create a temporary table with specific database name,
			// table name and schema
			HCatOutputFormat.setOutput(outputConf, null,
					OutputJobInfo.create(dbName, tblName, null));
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		rdd.mapToPair(new PairFunction<HCatRecord, NullWritable, HCatRecord>() {
			private static final long serialVersionUID = -7764723533860377882L;

			@Override
			public Tuple2<NullWritable, HCatRecord> call(HCatRecord record)
					throws Exception {
				return new Tuple2<NullWritable, HCatRecord>(NullWritable.get(),
						record);
			}
		}).saveAsNewAPIHadoopDataset(outputConf);
	}

}
