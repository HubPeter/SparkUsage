package iie.hadoop.spark.ops;

import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.spark.api.java.JavaRDD;

public interface StoreToTableOp {

	void toTable(JavaRDD<HCatRecord> records, String dbName, String tblName);

}
