package com.pic.ala.storm;

import java.util.HashMap;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.apache.storm.topology.TopologyBuilder;

import com.pic.ala.storm.bolt.ESIndexBoltForLog;
import com.pic.ala.storm.translator.ApLogRecordTranslator;

/**
 * This topology consumes application logstream from Kafka,
 * and then indexes them into Elasticsearch.
 *
 * @author gary
 * @since  2017年4月21日 下午4:34:23
 */
public class ApLogAnalyzer extends LogBaseTopology {

	private static boolean DEBUG = false;

//	private static final Logger LOG = Logger.getLogger(ApLogAnalyzer.class);

	private static final String KAFKA_SPOUT_ID = "kafkaSpout";
	private static final String ESINDEXER_BOLT_ID = "ESIndexerBolt";
	private static final String CONSUMER_GROUP_ID = "aplog-analyzer";

	public ApLogAnalyzer(String configFileLocation) throws Exception {
		super(configFileLocation);
	}

	private KafkaSpoutConfig<String, String> constructKafkaSpoutConf() {
//		final BrokerHosts hosts = new ZkHosts(topologyConfig.getProperty("kafka.zookeeper.host.port"));
		final String bootstrapServers = topologyConfig.getProperty("metadata.broker.list");
		final String topic = topologyConfig.getProperty("kafka.topic");
//		final String zkRoot = topologyConfig.getProperty("kafka.zkRoot");
//		String consumerGroupId = UUID.randomUUID().toString();
//		final SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, CONSUMER_GROUP_ID);
//		spoutConfig.startOffsetTime = System.currentTimeMillis();
//		spoutConfig.scheme = new SchemeAsMultiScheme(apLogScheme);
//		spoutConfig.retryInitialDelayMs = 10000;	// 10 seconds
//		spoutConfig.retryDelayMultiplier = 1.1;		// 10, 11, 12.1, 13.31, 14.641...
//		spoutConfig.retryDelayMaxMs = 590000;		// about 10 minutes

		ApLogRecordTranslator<String, String> apLogRecordTranslator = new ApLogRecordTranslator<>();

		/**
		 * 以下參數設定會影響到 KafkaSpout 的效能，所以請參考：
		 * https://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.6.0/bk_storm-component-guide/content/storm-kafkaspout-perf.html
		 */
		final KafkaSpoutConfig<String, String> spoutConfig = KafkaSpoutConfig.builder(bootstrapServers, topic)
			.setGroupId(CONSUMER_GROUP_ID)
			.setOffsetCommitPeriodMs(5_000L)		// offset 的 commit 週期（單位：毫秒）
			.setMaxUncommittedOffsets(10_000_000)	// 未 commit 的 offset 數量最大值（越大，會越使用越多記憶體）
			.setMaxPollRecords(100)					// 每次輪詢最多可抓幾筆 records？
			.setPollTimeoutMs(10_000L)				// 當輪詢沒有資料時，要等待多久（單位：毫秒）？
			.setRetry(new KafkaSpoutRetryExponentialBackoff(TimeInterval.seconds(10), TimeInterval.milliSeconds(2000),
					KafkaSpoutConfig.DEFAULT_MAX_RETRIES, TimeInterval.seconds(600)))
			.setRecordTranslator(apLogRecordTranslator)
			.build();

		return spoutConfig;
	}

	private void configureKafkaSpout(TopologyBuilder builder, Config config) {
		KafkaSpout<String, String> kafkaSpout = new KafkaSpout<>(constructKafkaSpoutConf());
		final int spoutThreads = Integer.valueOf(topologyConfig.getProperty("spout.KafkaSpout.threads"));

		builder.setSpout(KAFKA_SPOUT_ID, kafkaSpout, spoutThreads).setDebug(DEBUG);
	}

	private void configureESBolts(TopologyBuilder builder, Config config) {
		HashMap<String, Object> esConfig = new HashMap<String, Object>();
		esConfig.put(ESIndexBoltForLog.ES_CLUSTER_NAME, topologyConfig.getProperty(ESIndexBoltForLog.ES_CLUSTER_NAME));
		esConfig.put(ESIndexBoltForLog.ES_NODES, topologyConfig.getProperty(ESIndexBoltForLog.ES_NODES));
		esConfig.put(ESIndexBoltForLog.ES_SHIELD_ENABLED, topologyConfig.getProperty(ESIndexBoltForLog.ES_SHIELD_ENABLED));
		esConfig.put(ESIndexBoltForLog.ES_SHIELD_USER, topologyConfig.getProperty(ESIndexBoltForLog.ES_SHIELD_USER));
		esConfig.put(ESIndexBoltForLog.ES_SHIELD_PASS, topologyConfig.getProperty(ESIndexBoltForLog.ES_SHIELD_PASS));
		esConfig.put(ESIndexBoltForLog.ES_ASYNC_ENABLED, topologyConfig.getProperty(ESIndexBoltForLog.ES_ASYNC_ENABLED));
		config.put("es.conf", esConfig);
		ESIndexBoltForLog esBolt = new ESIndexBoltForLog().withConfigKey("es.conf");
		final int boltThreads = Integer.valueOf(topologyConfig.getProperty("bolt.ESIndexerBolt.threads"));

		builder.setBolt(ESINDEXER_BOLT_ID, esBolt, boltThreads).shuffleGrouping(KAFKA_SPOUT_ID).setDebug(DEBUG);
	}

	private void buildAndSubmit() throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		final int numWorkers = Integer.valueOf(topologyConfig.getProperty("num.workers"));
		Config config = new Config();
		config.setDebug(DEBUG);
		config.setNumWorkers(numWorkers);
		config.setMaxSpoutPending(1000000);
		// https://github.com/apache/storm/tree/v0.10.0/external/storm-kafka
		config.setMessageTimeoutSecs(600);	// This value(30 secs by default) must
							// be larger than retryDelayMaxMs
							// (60 secs by default) in
							// KafkaSpout.
		TopologyBuilder builder = new TopologyBuilder();
		configureKafkaSpout(builder, config);
		configureESBolts(builder, config);
//		LocalCluster cluster = new LocalCluster();
		StormSubmitter.submitTopology("ApLogAnalyzerV1", config, builder.createTopology());
	}

	public static void main(String args[]) throws Exception {
		final String configFileLocation = "ApLogAnalyzer.properties";
		ApLogAnalyzer topology = new ApLogAnalyzer(configFileLocation);
		topology.buildAndSubmit();
	}

}
