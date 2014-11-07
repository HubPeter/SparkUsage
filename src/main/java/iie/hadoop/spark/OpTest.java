package iie.hadoop.spark;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema.Type;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.thrift.TException;

import scala.Tuple2;

import com.google.common.collect.Lists;

public class OpTest {

	public static void main(String[] args) {
		JavaSparkContext jsc = new JavaSparkContext(new SparkConf());

		LoadOp loadOp = null;
		TransformOp[] transformOps = null;
		StoreOp storeOp = null;

		RDDWithSchema records = loadOp.load(jsc, null);
		for (TransformOp transformOp : transformOps) {
			records = transformOp.transform(jsc, null,
					Lists.newArrayList(records));
		}
		storeOp.store(jsc, null, records);

		jsc.stop();
	}

	public static class RDDWithSchema {
		public final HCatSchema schema;
		public final JavaRDD<HCatRecord> rdd;

		public RDDWithSchema(HCatSchema schema, JavaRDD<HCatRecord> rdd) {
			this.schema = schema;
			this.rdd = rdd;
		}
	}

	public interface Configurable {
		List<String> getKeys();
	}

	public interface LoadOp extends Configurable {
		RDDWithSchema load(JavaSparkContext jsc, Configuration conf);
	}

	public interface TransformOp extends Configurable {
		RDDWithSchema transform(JavaSparkContext jsc, Configuration conf,
				List<RDDWithSchema> rdds);
	}

	public interface StoreOp extends Configurable {
		void store(JavaSparkContext jsc, Configuration conf, RDDWithSchema rdd);
	}

	public static class LoadFromTable implements LoadOp {
		public static final String DATABASE_NAME = "database.name";
		public static final String TABLE_NAME = "table.name";

		@Override
		public List<String> getKeys() {
			return Lists.newArrayList(DATABASE_NAME, TABLE_NAME);
		}

		@Override
		public RDDWithSchema load(JavaSparkContext jsc, Configuration conf) {
			String dbName = conf.get(DATABASE_NAME);
			String tblName = conf.get(TABLE_NAME);
			HCatSchema schema = null;
			try {
				HCatInputFormat.setInput(conf, dbName, tblName);
				schema = HCatInputFormat.getTableSchema(conf);
			} catch (IOException e) {
				e.printStackTrace();
			}
			JavaRDD<HCatRecord> rdd = jsc
					.newAPIHadoopRDD(conf, HCatInputFormat.class,
							WritableComparable.class, HCatRecord.class)
					.map(new Function<Tuple2<WritableComparable, HCatRecord>, HCatRecord>() {
						private static final long serialVersionUID = -2362812254158054659L;

						@Override
						public HCatRecord call(
								Tuple2<WritableComparable, HCatRecord> v)
								throws Exception {
							return v._2;
						}
					});
			return new RDDWithSchema(schema, rdd);
		}

	}

	public static class Token implements TransformOp {

		public static String TOKEN_COLUMNS = "token.columns";

		public static HCatSchema tokenSubSchema;
		static {
			try {
				tokenSubSchema = new HCatSchema(
						Lists.newArrayList(new HCatFieldSchema(null,
								Type.STRING, null)));
			} catch (HCatException e) {
				e.printStackTrace();
			}
		}

		@Override
		public List<String> getKeys() {
			return Lists.newArrayList(TOKEN_COLUMNS);
		}

		@Override
		public RDDWithSchema transform(JavaSparkContext jsc,
				Configuration conf, List<RDDWithSchema> rdds) {
			String tokenColsStr = conf.get(TOKEN_COLUMNS);
			final String[] tokenCols = tokenColsStr.split(",");
			if (rdds.size() != 1) {
				return null;
			}
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
			JavaRDD<HCatRecord> newRDD = rdd
					.map(new Function<HCatRecord, HCatRecord>() {
						private static final long serialVersionUID = 5110377890285238705L;

						@Override
						public HCatRecord call(HCatRecord record)
								throws Exception {
							DefaultHCatRecord newRecord = new DefaultHCatRecord(
									record.size() + tokenCols.length);
							int index = 0;
							for (Object value : record.getAll()) {
								newRecord.set(index++, value);
							}
							for (String tokenCol : tokenCols) {
								String[] tokens = record.get(tokenCol, schema)
										.toString().split(" ");
								newRecord.set(index++,
										Lists.newArrayList(tokens));
							}
							return newRecord;
						}

					});
			return new RDDWithSchema(newSchema, newRDD);
		}

	}

	public static class StoreToTable implements StoreOp {

		public static final String DATABASE_NAME = "database.name";
		public static final String TABLE_NAME = "table.name";

		@Override
		public List<String> getKeys() {
			return Lists.newArrayList(DATABASE_NAME, TABLE_NAME);
		}

		@Override
		public void store(JavaSparkContext jsc, Configuration conf,
				RDDWithSchema rdd) {
			String dbName = conf.get(DATABASE_NAME,
					MetaStoreUtils.DEFAULT_DATABASE_NAME);
			String tblName = conf.get(TABLE_NAME);
			try {
				HiveMetaStoreClient client = HCatUtil
						.getHiveClient(new HiveConf());
				Table table = new Table();
				table.setDbName(dbName);
				table.setTableName(tblName);

				StorageDescriptor sd = new StorageDescriptor();
				List<FieldSchema> cols = HCatUtil.getFieldSchemaList(rdd.schema
						.getFields());
				sd.setCols(cols);
				table.setSd(sd);
				client.createTable(table);
				HCatOutputFormat.setOutput(conf, null,
						OutputJobInfo.create(dbName, tblName, null));
			} catch (IOException | TException e) {
				e.printStackTrace();
			}
			rdd.rdd.mapToPair(
					new PairFunction<HCatRecord, NullWritable, HCatRecord>() {
						private static final long serialVersionUID = -4658431554556766962L;

						@Override
						public Tuple2<NullWritable, HCatRecord> call(
								HCatRecord record) throws Exception {
							return new Tuple2<NullWritable, HCatRecord>(
									NullWritable.get(), record);
						}
					}).saveAsNewAPIHadoopDataset(conf);
		}

	}
}
