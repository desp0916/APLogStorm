/**
 * Ref: https://github.com/apache/storm/tree/master/external/storm-kafka
 *
 * 1. Create a table with HBase shell:
 *
 *     create 'aes3g', 'cf'
 *
 * 2. Submit this topology to consume the topic on Kafka and ingest into Hbase:
 *
 *     storm jar target/LearnStorm-0.0.1-SNAPSHOT.jar com.pic.ala.ApLogAnalyzer
 *
 * 3. Understanding the Parallelism of a Storm Topology
 *
 *   http://www.michael-noll.com/blog/2012/10/16/understanding-the-parallelism-of-a-storm-topology/
 *
 * 4. KafkaSpout 浅析
 *
 *   http://www.cnblogs.com/cruze/p/4241181.html
 *
 * 5. Unofficial Storm and Kafka Best Practices Guide
 *
 *   https://community.hortonworks.com/articles/550/unofficial-storm-and-kafka-best-practices-guide.html
 */

package com.pic.ala;

import java.util.HashMap;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.apache.storm.topology.TopologyBuilder;

import com.pic.ala.bolt.ESIndexerBolt;
import com.pic.ala.scheme.ApLogScheme;

public class ApLogAnalyzer extends LogBaseTopology {

	private static boolean DEBUG = false;

//	private static final Logger LOG = Logger.getLogger(ApLogAnalyzer.class);

	private static final String KAFKA_SPOUT_ID = "kafkaSpout";
	private static final String ESINDEXER_BOLT_ID = "ESIndexerBolt";
	private static final String CONSUMER_GROUP_ID = "aplog-analyzer";
	private ApLogScheme apLogScheme;

	public ApLogAnalyzer(String configFileLocation) throws Exception {
		super(configFileLocation);
		this.apLogScheme = new ApLogScheme();
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
		esConfig.put(ESIndexerBolt.ES_CLUSTER_NAME, topologyConfig.getProperty(ESIndexerBolt.ES_CLUSTER_NAME));
		esConfig.put(ESIndexerBolt.ES_NODES, topologyConfig.getProperty(ESIndexerBolt.ES_NODES));
		esConfig.put(ESIndexerBolt.ES_SHIELD_ENABLED, topologyConfig.getProperty(ESIndexerBolt.ES_SHIELD_ENABLED));
		esConfig.put(ESIndexerBolt.ES_SHIELD_USER, topologyConfig.getProperty(ESIndexerBolt.ES_SHIELD_USER));
		esConfig.put(ESIndexerBolt.ES_SHIELD_PASS, topologyConfig.getProperty(ESIndexerBolt.ES_SHIELD_PASS));
		esConfig.put(ESIndexerBolt.ES_ASYNC_ENABLED, topologyConfig.getProperty(ESIndexerBolt.ES_ASYNC_ENABLED));
		config.put("es.conf", esConfig);
		ESIndexerBolt esBolt = new ESIndexerBolt().withConfigKey("es.conf");
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
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("ApLogAnalyzerV1", config, builder.createTopology());
	}

	public static void main(String args[]) throws Exception {
		final String configFileLocation = "ApLogAnalyzer.properties";
		ApLogAnalyzer topology = new ApLogAnalyzer(configFileLocation);
		topology.buildAndSubmit();
	}

}
