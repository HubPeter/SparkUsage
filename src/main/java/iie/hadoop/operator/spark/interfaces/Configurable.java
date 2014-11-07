package iie.hadoop.operator.spark.interfaces;

import java.util.List;

/**
 *
 * 用于获取算子可配置项列表。
 * 
 * @author weixing
 * 
 */
public interface Configurable {
	List<String> getKeys();
}
