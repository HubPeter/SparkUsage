package iie.hadoop.mapreduce.usage;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

public class HCatOutputFormatTest {
	public static void main(String[] args) {
		Configuration outputConf = new Configuration();
		outputConf.set("hive.metastore.uris", "thrift://m105:9083");
		try {
			HCatOutputFormat.setOutput(outputConf, null,
					OutputJobInfo.create("wx", "tbl2", null));
		} catch (IOException e) {
			e.printStackTrace();
		}
		for (Entry<String, String> entry : outputConf) {
			System.out.println(entry.getKey() + ": " + entry.getValue());
		}
	}
}
