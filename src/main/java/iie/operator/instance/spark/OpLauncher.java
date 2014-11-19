package iie.operator.instance.spark;

import iie.operator.api.spark.LoadOp;
import iie.operator.api.spark.RDDWithSchema;
import iie.operator.api.spark.StoreOp;
import iie.operator.api.spark.TransformOp;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.google.common.collect.Lists;

public class OpLauncher {
	public static enum OperatorType {
		load, transform, store;
	}

	public static class Operator {
		private final String name;
		private final OperatorType type;
		private final Configuration conf;
		private final List<Operator> sources;

		public Operator(String name, OperatorType type,
				Configuration configuration) {
			this.name = name;
			this.type = type;
			this.conf = configuration;
			this.sources = new LinkedList<Operator>();
		}

		public String getName() {
			return name;
		}

		public OperatorType getType() {
			return type;
		}

		public Configuration getConf() {
			return conf;
		}

		public void addSources(Operator operator) {
			sources.add(operator);
		}

		public List<Operator> getSources() {
			return sources;
		}

		public String toString() {
			return "{name: " + name + ", type: " + type + ", sources: "
					+ sources + "}";
		}
	}

	public static void main(String[] args) {
		Collection<Operator> operators = null;
		try {
			operators = buildOperatorFromXml("src/main/resources/task-example.xml");
			System.out.println(operators);
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		Map<Operator, List<RDDWithSchema>> rdds = new HashMap<Operator, List<RDDWithSchema>>();
		JavaSparkContext jsc = new JavaSparkContext(new SparkConf());
		try {
			for (Operator operator : operators) {
				if (!rdds.containsKey(operator)) {
					createRDD(jsc, rdds, operator);
				}
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	public static void createRDD(JavaSparkContext jsc,
			Map<Operator, List<RDDWithSchema>> rdds, Operator operator)
			throws ClassNotFoundException {
		List<RDDWithSchema> sources = new LinkedList<RDDWithSchema>();
		for (Operator source : operator.getSources()) {
			if (!rdds.containsKey(source)) {
				createRDD(jsc, rdds, operator);
			}
			sources.addAll(rdds.get(source));
		}

		if (operator.getType().equals(OperatorType.load)) {
			Class<? extends LoadOp> opClass = (Class<? extends LoadOp>) Class
					.forName(operator.getName());
			LoadOp loadOp = ReflectionUtils.newInstance(opClass, null);
			RDDWithSchema rdd = loadOp.load(jsc, operator.getConf());
			rdds.put(operator, Lists.newArrayList(rdd));
		} else if (operator.getType().equals(OperatorType.transform)) {
			Class<? extends TransformOp> transClass = (Class<? extends TransformOp>) Class
					.forName(operator.getName());
			TransformOp transOp = ReflectionUtils.newInstance(transClass, null);
			rdds.put(operator,
					transOp.transform(jsc, operator.getConf(), sources));
		} else if (operator.getType().equals(OperatorType.store)) {
			Class<? extends StoreOp> storeClass = (Class<? extends StoreOp>) Class
					.forName(operator.getName());
			for (RDDWithSchema source : sources) {
				StoreOp storeOp = ReflectionUtils.newInstance(storeClass, null);
				storeOp.store(jsc, operator.getConf(), source);
			}
		}
	}

	public static Collection<Operator> buildOperatorFromXml(String path)
			throws ParserConfigurationException, SAXException, IOException {
		DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory
				.newInstance();
		DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
		Document document = builder.parse(new File(path));
		NodeList opNodes = document.getDocumentElement().getElementsByTagName(
				"operator");
		Map<String, Operator> operators = new HashMap<String, Operator>(
				opNodes.getLength());
		Map<String, String> dependents = new HashMap<String, String>(
				opNodes.getLength());
		for (int i = 0; i < opNodes.getLength(); ++i) {
			Element opNode = (Element) opNodes.item(i);
			String id = opNode.getElementsByTagName("id").item(0)
					.getTextContent();
			String name = opNode.getElementsByTagName("name").item(0)
					.getTextContent();
			Configuration conf = new Configuration();
			NodeList props = ((Element) opNode.getElementsByTagName(
					"configuration").item(0)).getElementsByTagName("property");
			for (int j = 0; j < props.getLength(); ++j) {
				Element prop = (Element) props.item(j);
				String key = prop.getElementsByTagName("name").item(0)
						.getTextContent();
				String value = prop.getElementsByTagName("value").item(0)
						.getTextContent();
				conf.set(key, value);
			}
			OperatorType type = OperatorType.valueOf(opNode
					.getElementsByTagName("type").item(0).getTextContent());

			Operator operator = new Operator(name, type, conf);
			operators.put(id, operator);

			NodeList sources = opNode.getElementsByTagName("sources");
			if (sources.getLength() != 0) {
				dependents.put(id, sources.item(0).getTextContent());
			}
		}

		for (Entry<String, String> dependent : dependents.entrySet()) {
			String[] strs = dependent.getValue().split(",");
			Operator operator = operators.get(dependent.getKey());
			for (String str : strs) {
				Operator source = operators.get(str);
				operator.addSources(source);
			}
		}

		return operators.values();
	}
}
