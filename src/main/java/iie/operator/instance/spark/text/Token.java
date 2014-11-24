package iie.operator.instance.spark.text;

import iie.operator.api.spark.text.TransformOp;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.google.common.collect.Lists;

/**
 * 
 * 实现的一个用于对指定列进行分词的Spark转换算子。
 * 
 * @author weixing
 */
public class Token extends TransformOp {
	private static final long serialVersionUID = -7919283282580504900L;
	public static String TOKEN_COLUMNS = "token.columns";

	@Override
	public List<JavaRDD<String>> transform(JavaSparkContext jsc,
			Configuration conf, List<JavaRDD<String>> rdds) {
		// 获取用户设置的要分词的列
		String[] tokenCols = conf.get(TOKEN_COLUMNS).split(",");
		final int[] tokenInxs = new int[tokenCols.length];
		for (int i = 0; i < tokenInxs.length; ++i) {
			tokenInxs[i] = Integer.valueOf(tokenCols[i]);
		}
		List<JavaRDD<String>> results = new ArrayList<JavaRDD<String>>(
				rdds.size());
		for (JavaRDD<String> rdd : rdds) {
			JavaRDD<String> newRDD = rdd.map(new Function<String, String>() {
				private static final long serialVersionUID = 5110377890285238705L;

				@Override
				public String call(String record) throws Exception {
					// 由于RDD中每一条记录都是以String表示的，
					// 因此需要先进行处理，得到每个字段的值。
					// 每个转换算子都需要此操作。
					String result = record;
					String[] values = record.split("\t");
					for (int tokenInx : tokenInxs) {
						result += values[tokenInx].split(" ") + "\t";
					}
					return result;
				}

			});
			results.add(newRDD);
		}
		return results;
	}

}
