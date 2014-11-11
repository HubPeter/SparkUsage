package iie.hadoop.operator.spark.instance;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema.Type;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.google.common.collect.Lists;

import iie.hadoop.operator.spark.interfaces.RDDWithSchema;
import iie.hadoop.operator.spark.interfaces.TransformOp;

/**
 * 
 * 实现的一个用于对指定列进行分词的Spark转换算子。
 * 
 * 通过TOKEN_COLUMNS配置需要分词的列名，比如“col1,col2”。
 * 
 * 该算子会为分词结果形成新的列，同时会生成新的schema，即将添加新生成的列信息。
 * 
 * @author weixing
 *
 */
public class Token implements TransformOp, Serializable {
	private static final long serialVersionUID = -5052011103015578631L;
	public static String TOKEN_COLUMNS = "token.columns";
	public static HCatSchema tokenSubSchema;
	static {
		try {
			tokenSubSchema = new HCatSchema(
					Lists.newArrayList(new HCatFieldSchema(null,
							TypeInfoFactory.stringTypeInfo, null)));
		} catch (HCatException e) {
			e.printStackTrace();
		}
	}

	@Override
	public List<String> getKeys() {
		return Lists.newArrayList(TOKEN_COLUMNS);
	}

	@Override
	public RDDWithSchema transform(JavaSparkContext jsc, Configuration conf,
			List<RDDWithSchema> rdds) {
		// 获取用户设置的要分词的列
		String tokenColsStr = conf.get(TOKEN_COLUMNS);
		final String[] tokenCols = tokenColsStr.split(",");
		if (rdds.size() != 1) {
			return null;
		}
		// 生成分词后的schema
		final HCatSchema schema = rdds.get(0).schema;
		JavaRDD<HCatRecord> rdd = rdds.get(0).rdd;
		HCatSchema newSchema = new HCatSchema(schema.getFields());
		for (String tokenCol : tokenCols) {
			HCatFieldSchema fieldSchema;
			try {
				fieldSchema = new HCatFieldSchema(tokenCol + ".token",
						Type.ARRAY, tokenSubSchema, "");
				newSchema.append(fieldSchema);
			} catch (HCatException e) {
				e.printStackTrace();
			}
		}

		// 对指定列进行分词，生成新的RDD
		JavaRDD<HCatRecord> newRDD = rdd
				.map(new Function<HCatRecord, HCatRecord>() {
					private static final long serialVersionUID = 5110377890285238705L;

					@Override
					public HCatRecord call(HCatRecord record) throws Exception {
						DefaultHCatRecord newRecord = new DefaultHCatRecord(
								record.size() + tokenCols.length);
						int index = 0;
						for (Object value : record.getAll()) {
							newRecord.set(index++, value);
						}
						for (String tokenCol : tokenCols) {
							String[] tokens = record.get(tokenCol, schema)
									.toString().split(" ");
							newRecord.set(index++, Lists.newArrayList(tokens));
						}
						return newRecord;
					}

				});
		return new RDDWithSchema(newSchema, newRDD);
	}

}
