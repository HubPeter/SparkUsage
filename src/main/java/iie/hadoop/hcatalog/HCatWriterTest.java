package iie.hadoop.hcatalog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatWriter;
import org.apache.hive.hcatalog.data.transfer.WriteEntity;
import org.apache.hive.hcatalog.data.transfer.WriterContext;

public class HCatWriterTest {
	public static final int BATCH_SIZE = 10;

	public static void main(String[] args) throws HCatException {
		WriteEntity.Builder builder = new WriteEntity.Builder();
		WriteEntity entity = builder.withDatabase("wx")
				.withTable("tbl_spark_in").build();
		List<Object> record = new ArrayList<Object>(2);
		record.add(1);
		record.add("wei xing");

		Map<String, String> config = new HashMap<String, String>();
		HCatWriter writer = DataTransferFactory.getHCatWriter(entity, config);
		WriterContext context = writer.prepareWrite();
		HCatWriter splitWriter = DataTransferFactory.getHCatWriter(context);
		List<HCatRecord> records = new ArrayList<HCatRecord>(BATCH_SIZE);
		for (int i = 0; i < BATCH_SIZE; ++i) {
			records.add(new DefaultHCatRecord(record));
		}
		splitWriter.write(records.iterator());
		writer.commit(context);
	}

}
