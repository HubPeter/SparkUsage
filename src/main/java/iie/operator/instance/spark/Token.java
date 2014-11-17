package iie.operator.instance.spark;

import iie.operator.api.spark.RDDWithSchema;
import iie.operator.api.spark.TransformOp;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema.Type;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.spark.SerializableWritable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.google.common.collect.Lists;

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
public class Token extends TransformOp {
	private static final long serialVersionUID = -7919283282580504900L;
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

	public Token() {
		super(null);
	}

	@Override
	public List<RDDWithSchema> transform(JavaSparkContext jsc,
			Configuration conf, List<RDDWithSchema> rdds) {
		// 获取用户设置的要分词的列
		final String[] tokenCols = conf.get(TOKEN_COLUMNS).split(",");
		List<RDDWithSchema> results = new ArrayList<RDDWithSchema>(rdds.size());
		for (RDDWithSchema rdd : rdds) {
			// 生成分词后的schema
			final HCatSchema oldSchema = rdd.getSchema();
			HCatSchema newSchema = new HCatSchema(oldSchema.getFields());
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
			JavaRDD<SerializableWritable<HCatRecord>> newRDD = rdd
					.getRecords()
					.map(new Function<SerializableWritable<HCatRecord>, SerializableWritable<HCatRecord>>() {
						private static final long serialVersionUID = 5110377890285238705L;

						@Override
						public SerializableWritable<HCatRecord> call(
								SerializableWritable<HCatRecord> record)
								throws Exception {
							DefaultHCatRecord newRecord = new DefaultHCatRecord(
									record.value().size() + tokenCols.length);
							int index = 0;
							for (Object value : record.value().getAll()) {
								newRecord.set(index++, value);
							}
							for (String tokenCol : tokenCols) {
								String[] tokens = record.value()
										.get(tokenCol, oldSchema).toString()
										.split(" ");
								newRecord.set(index++,
										Lists.newArrayList(tokens));
							}
							return new SerializableWritable<HCatRecord>(
									newRecord);
						}

					});
			results.add(new RDDWithSchema(null, newSchema, newRDD));
		}
		return results;
	}

}
