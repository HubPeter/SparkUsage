package iie.hadoop.hcatalog;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatReader;
import org.apache.hive.hcatalog.data.transfer.ReadEntity;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;

public class HCatReaderTest {

	public static void main(String[] args) throws HCatException {
		ReadEntity.Builder builder = new ReadEntity.Builder();
		ReadEntity entity = builder.withDatabase("wx")
				.withTable("tbl_spark_out").build();
		Map<String, String> config = new HashMap<String, String>();
		HCatReader reader = DataTransferFactory.getHCatReader(entity, config);
		ReaderContext cntxt = reader.prepareRead();

		// cntxt 可以在多线程、多进程或者多节点中使用。

		for (int i = 0; i < cntxt.numSplits(); ++i) {
			HCatReader splitReader = DataTransferFactory
					.getHCatReader(cntxt, i);
			Iterator<HCatRecord> itr1 = splitReader.read();
			while (itr1.hasNext()) {
				HCatRecord record = itr1.next();
				System.out.println(record.getAll());
			}
		}
	}
}
