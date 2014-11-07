package iie.hadoop.spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.api.java.DataType;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructField;
import org.apache.spark.sql.api.java.StructType;

import scala.Tuple2;

public class SparkUtils {
	public static HCatSchema getHCatSchema(String[] fieldNames,
			String[] fieldTypes) throws HCatException {
		List<HCatFieldSchema> fieldSchemas = new ArrayList<HCatFieldSchema>(
				fieldNames.length);

		for (int i = 0; i < fieldNames.length; ++i) {
			HCatFieldSchema.Type type = HCatFieldSchema.Type
					.valueOf(fieldTypes[i].toUpperCase());
			fieldSchemas
					.add(new HCatFieldSchema(fieldNames[i], type, ""));
		}
		return new HCatSchema(fieldSchemas);
	}

	public static StructType getStructType(String[] fieldNames,
			String[] fieldTypes) {
		List<StructField> fields = new ArrayList<StructField>(fieldNames.length);
		return DataType.createStructType(fields);
	}

	public static HCatRecord rowToHCatRecord(Row row) {
		DefaultHCatRecord record = new DefaultHCatRecord();
		for (int i = 0; i < row.length(); ++i) {
			record.set(i, row.get(i));
		}
		return record;
	}

	public static Row hcatRecordToRow(HCatRecord record) {
		Row row = Row.create(record.getAll());
		return row;
	}

	public static JavaRDD<HCatRecord> readFromTextFile(JavaSparkContext jsc,
			String path) {
		return jsc.textFile(path).map(new Function<String, HCatRecord>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 3304807095529363352L;

			@Override
			public HCatRecord call(String line) throws Exception {
				HCatRecord record = new DefaultHCatRecord();

				String[] fields = line.split(",", -1);
				for (int i = 0; i < fields.length; ++i) {
					record.set(i, fields[i]);
				}

				return record;
			}

		});
	}

	public static JavaRDD<HCatRecord> sparkSQL(JavaSparkContext jsc,
			JavaRDD<HCatRecord> rdd, StructType schema, String tblName,
			String sql) {
		JavaSQLContext jssc = new JavaSQLContext(jsc);
		JavaSchemaRDD temp = jssc.applySchema(
				rdd.map(new Function<HCatRecord, Row>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = -2691352236555642068L;

					@Override
					public Row call(HCatRecord record) throws Exception {
						return Row.create(record.getAll());
					}

				}), schema);

		jssc.registerRDDAsTable(temp, tblName);
		return jssc.sql(sql).map(new Function<Row, HCatRecord>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = -8730215956163612368L;

			@Override
			public HCatRecord call(Row row) throws Exception {
				return rowToHCatRecord(row);
			}

		});
	}

	public static JavaRDD<HCatRecord> readFromTable(JavaSparkContext jsc,
			String dbName, String tblName) throws IOException {
		Configuration inputConf = new Configuration();
		/* need be set, if hive configuration file can not be visible */
		inputConf.set("hive.metastore.uris", "thrift://m105:9083");
		HCatInputFormat.setInput(inputConf, dbName, tblName);

		/* set columns which will be read */
		// List<Integer> readColumns = Lists.newArrayList(0, 1);
		// inputConf.setInt(RCFile.COLUMN_NUMBER_CONF_STR, readColumns.size());
		// ColumnProjectionUtils.appendReadColumns(inputConf, readColumns);
		return jsc
				.newAPIHadoopRDD(inputConf, HCatInputFormat.class,
						WritableComparable.class, HCatRecord.class)
				.map(new Function<Tuple2<WritableComparable, HCatRecord>, HCatRecord>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 271668729623304968L;

					@Override
					public HCatRecord call(
							Tuple2<WritableComparable, HCatRecord> v)
							throws Exception {
						return v._2;
					}
				});
	}

	public static void saveToTable(JavaRDD<HCatRecord> rdd, String dbName,
			String tblName) throws IOException {
		Configuration outputConf = new Configuration();
		outputConf.set("hive.metastore.uris", "thrift://m105:9083");
		HCatOutputFormat.setOutput(outputConf, null,
				OutputJobInfo.create(dbName, tblName, null));
		rdd.mapToPair(
				new PairFunction<HCatRecord, WritableComparable, HCatRecord>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 3530199468954019029L;

					@Override
					public Tuple2<WritableComparable, HCatRecord> call(
							HCatRecord record) throws Exception {
						return new Tuple2(NullWritable.get(), record);
					}
				}).saveAsNewAPIHadoopDataset(outputConf);
	}

	public static JavaRDD<HCatRecord> filter(JavaRDD<HCatRecord> rdd) {
		return rdd.filter(new Function<HCatRecord, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 7195500411538098019L;

			@Override
			public Boolean call(HCatRecord record) throws Exception {
				return record.get(1).toString().contains("hello");
			}

		});
	}

}
