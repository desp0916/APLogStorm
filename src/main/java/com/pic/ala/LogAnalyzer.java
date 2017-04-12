package com.pic.ala;

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

import com.pic.ala.bolt.ESIndexBolt;
import com.pic.ala.scheme.LogScheme;

public class LogAnalyzer extends LogBaseTopology {

	private static boolean DEBUG = false;

	private static final String KAFKA_SPOUT_ID = "kafkaSpout";
	private static final String ESINDEX_BOLT_ID = "ESIndexBolt";
	private static final String CONSUMER_GROUP_ID = "log-analyzer";
	private LogScheme logScheme;

	public LogAnalyzer(String configFileLocation) throws Exception {
		super(configFileLocation);
		logScheme = new LogScheme();
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

		final KafkaSpoutConfig<String, String> spoutConfig = KafkaSpoutConfig.builder(bootstrapServers, topic)
			.setGroupId(CONSUMER_GROUP_ID)
			.setMaxPollRecords(5)
			.setRetry(new KafkaSpoutRetryExponentialBackoff(TimeInterval.seconds(10), TimeInterval.milliSeconds(2000),
					KafkaSpoutConfig.DEFAULT_MAX_RETRIES, TimeInterval.seconds(10000)))
			.setMaxUncommittedOffsets(250)
			.setPollTimeoutMs(1000)
			.build();

		return spoutConfig;
	}

	private void configureKafkaSpout(TopologyBuilder builder, Config config) {
		KafkaSpout<String, String> kafkaSpout = new KafkaSpout<String, String>(constructKafkaSpoutConf());
		final int spoutThreads = Integer.valueOf(topologyConfig.getProperty("spout.KafkaSpout.threads"));

		builder.setSpout(KAFKA_SPOUT_ID, kafkaSpout, spoutThreads).setDebug(DEBUG);
	}

	private void configureESBolts(TopologyBuilder builder, Config config) {
		HashMap<String, Object> esConfig = new HashMap<String, Object>();
		esConfig.put(ESIndexBolt.ES_CLUSTER_NAME, topologyConfig.getProperty(ESIndexBolt.ES_CLUSTER_NAME));
		esConfig.put(ESIndexBolt.ES_NODES, topologyConfig.getProperty(ESIndexBolt.ES_NODES));
		esConfig.put(ESIndexBolt.ES_SHIELD_ENABLED, topologyConfig.getProperty(ESIndexBolt.ES_SHIELD_ENABLED));
		esConfig.put(ESIndexBolt.ES_SHIELD_USER, topologyConfig.getProperty(ESIndexBolt.ES_SHIELD_USER));
		esConfig.put(ESIndexBolt.ES_SHIELD_PASS, topologyConfig.getProperty(ESIndexBolt.ES_SHIELD_PASS));
		esConfig.put(ESIndexBolt.ES_INDEX_NAME, topologyConfig.getProperty(ESIndexBolt.ES_INDEX_NAME));
		esConfig.put(ESIndexBolt.ES_INDEX_TYPE, topologyConfig.getProperty(ESIndexBolt.ES_INDEX_TYPE));
		esConfig.put(ESIndexBolt.ES_ASYNC_ENABLED, topologyConfig.getProperty(ESIndexBolt.ES_ASYNC_ENABLED));
		config.put("es.conf", esConfig);
		ESIndexBolt esBolt = new ESIndexBolt().withConfigKey("es.conf");
		final int boltThreads = Integer.valueOf(topologyConfig.getProperty("bolt.ESIndexBolt.threads"));

		builder.setBolt(ESINDEX_BOLT_ID, esBolt, boltThreads).shuffleGrouping(KAFKA_SPOUT_ID).setDebug(DEBUG);
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
							/// KafkaSpout.

		TopologyBuilder builder = new TopologyBuilder();
		configureKafkaSpout(builder, config);
		configureESBolts(builder, config);

//		LocalCluster cluster = new LocalCluster();
		StormSubmitter.submitTopology("LogAnalyzerV1", config, builder.createTopology());
	}

	public static void main(String args[]) throws Exception {
		final String configFileLocation = "LogAnalyzer.properties";
		LogAnalyzer topology = new LogAnalyzer(configFileLocation);
		topology.buildAndSubmit();
	}

}
