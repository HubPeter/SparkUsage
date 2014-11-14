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

	public static void main(String[] args) {
		WriteEntity.Builder builder = new WriteEntity.Builder();
		WriteEntity entity = builder.withDatabase("mydb").withTable("mytbl")
				.build();
		Map<String, String> config = new HashMap<String, String>();
		HCatWriter writer = DataTransferFactory.getHCatWriter(entity, config);
		try {
			WriterContext context = writer.prepareWrite();
			List<HCatRecord> records = new ArrayList<HCatRecord>(BATCH_SIZE);
			for (int i = 0; i < BATCH_SIZE; ++i) {
				records.add(new DefaultHCatRecord());
			}
			writer.write(records.iterator());
			writer.commit(context);
		} catch (HCatException e) {
			e.printStackTrace();
		}
	}

}
