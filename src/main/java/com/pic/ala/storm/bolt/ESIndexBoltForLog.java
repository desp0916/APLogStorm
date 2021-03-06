/**
 * ElasticSearch 最新版的作法：
 * https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/transport-client.html
 *
 * Adding mapping to a type from Java - how do I do it?
 * http://stackoverflow.com/questions/22071198/adding-mapping-to-a-type-from-java-how-do-i-do-it
 *
 * At first, you should create the index just like this:
 *  curl -XPUT 'localhost:9200/aplog_aes3g?pretty'
 *  curl -XPUT 'localhost:9200/aplog_pos?pretty'
 *  curl -XPUT 'localhost:9200/aplog_upcc?pretty'
 *  curl -XPUT 'localhost:9200/aplog_wds?pretty'
 *
 * ElasticSearch - Index document:
 * https://www.elastic.co/guide/en/elasticsearch/client/java-api/1.7/index-doc.html
 */

package com.pic.ala.storm.bolt;

import static com.pic.ala.util.LogUtil.isDateValid;
import static com.pic.ala.util.LogUtil.isNullOrEmpty;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pic.ala.storm.translator.LogRecordTranslator;

public class ESIndexBoltForLog extends BaseRichBolt {

	private static final long serialVersionUID = 3679185896129567534L;
	private static final Logger LOG = LoggerFactory.getLogger(ESIndexBoltForLog.class);
	private static final String ES_INDEX_PREFIX = "";
	private static Client client;
//	private static TransportClient transportClient;
	private OutputCollector collector;

	protected String configKey;

	public static final String ES_CLUSTER_NAME = "es.cluster.name";
	public static final String ES_NODES = "es.nodes";
	public static final String ES_SHIELD_ENABLED = "es.shield.enabled";
	public static final String ES_SHIELD_USER = "es.shield.user";
	public static final String ES_SHIELD_PASS = "es.shield.pass";
	public static final int MIN_CONNECTED_NODES = 5;
	public static final String ES_INDEX_NAME = "es.index.name";
	public static final String ES_INDEX_TYPE = "es.index.type";
	public static final String ES_ASYNC_ENABLED = "es.async.enabled";

	private String defaultIndex;
	private String defaultType;

	// DO NOT MODIFY HERE.
	// Instead modify the setting "es.async.enabled" in "LogAnalyzer.properties" file.
	private static boolean esAsyncEnabled = true;

	public ESIndexBoltForLog withConfigKey(final String configKey) {
		this.configKey = configKey;
		return this;
	}

	/**
	 * @TODO add mapping, see:
	 * http://stackoverflow.com/questions/22071198/adding-mapping-to-a-type-from-java-how-do-i-do-it
	 */
	@Override
	public void prepare(final Map stormConf, final TopologyContext context, final OutputCollector collector) {

		if (stormConf == null) {
			throw new IllegalArgumentException(
					"ElasticSearch configuration not found using key '" + this.configKey + "'");
		}

		Map<String, Object> conf = (Map<String, Object>) stormConf.get(this.configKey);

		String esClusterName = (String)conf.get(ES_CLUSTER_NAME);
		String esNodes = (String)conf.get(ES_NODES);

		esAsyncEnabled = Boolean.parseBoolean((String)conf.get(ES_ASYNC_ENABLED));
		boolean esShieldEnabled = Boolean.parseBoolean((String)conf.get(ES_SHIELD_ENABLED));
		String esShieldUser = (String)conf.get(ES_SHIELD_USER);
		String esShieldPass = (String)conf.get(ES_SHIELD_PASS);

		this.defaultIndex = (String)conf.get(ES_INDEX_NAME);
		this.defaultType = (String)conf.get(ES_INDEX_TYPE);

		if (esClusterName == null) {
			throw new IllegalArgumentException("No '" + ES_CLUSTER_NAME
				+ "' value found in configuration!");
		}

		if (esNodes == null) {
			throw new IllegalArgumentException("No '" + ES_NODES
				+ "' value found in configuration!");
		}

		if (esShieldEnabled && esShieldUser == null) {
			throw new IllegalArgumentException("No '" + ES_SHIELD_USER
				+ "' value found in configuration!");
		}

		if (esShieldEnabled && esShieldPass == null) {
			throw new IllegalArgumentException("No '" + ES_SHIELD_PASS
				+ "' value found in configuration!");
		}

		this.collector = collector;

		final Settings settings = Settings.builder()
				.put("cluster.name", esClusterName)
				.put("client.transport.sniff", true)
				.build();

		PreBuiltTransportClient preBuiltTransportClient = new PreBuiltTransportClient(settings);

		synchronized (ESIndexBoltForLog.class) {
			if (client == null) {
				List<String> esNodesList = Arrays.asList(esNodes.split("\\s*,\\s*"));
				for (String esNode : esNodesList) {
					try {

						preBuiltTransportClient.addTransportAddress(
								new InetSocketTransportAddress(InetAddress.getByName(esNode), 9300));
					} catch (Exception e) {
						LOG.warn("Unable to add ElasticSearch node: " + esNode);
					}
				}
				TransportClient transportClient = preBuiltTransportClient;
				client = transportClient;
			}
		}
	}

	/**
	 * http://storm.apache.org/documentation/Guaranteeing-message-processing.html
	 */
	@Override
	public void execute(Tuple tuple) {

		String index = (String) tuple.getValueByField(LogRecordTranslator.FIELD_INDEX);
		String type = (String) tuple.getValueByField(LogRecordTranslator.FIELD_TYPE);
		String logDate = (String) tuple.getValueByField(LogRecordTranslator.FIELD_LOG_DATE);
		String message = (String) tuple.getValueByField(LogRecordTranslator.FIELD_MESSAGE);
		String toBeIndexed = (String) tuple.getValueByField(LogRecordTranslator.FIELD_ES_SOURCE);

		if (isNullOrEmpty(index)) {
			index = defaultIndex;
		}
		if (isNullOrEmpty(type)) {
			type = defaultType;
		}
		if (!isDateValid(logDate, LogRecordTranslator.FORMAT_DATE)) {
			LOG.error("The format of logDate is null or invalid: {}", logDate);
			collector.ack(tuple);
			return;
		}
		if (isNullOrEmpty(logDate)	|| isNullOrEmpty(message) || isNullOrEmpty(toBeIndexed)) {
			LOG.error("Received null or incorrect value from tuple: {}", toBeIndexed);
			collector.ack(tuple);
			return;
		}

		if (client == null) {
			collector.fail(tuple);
			throw new RuntimeException("Unable to get ES client!");
		}

		try {
			if (esAsyncEnabled) {
				// Asynchronous way
				ListenableActionFuture<IndexResponse> future = client
						.prepareIndex(ES_INDEX_PREFIX + index.toLowerCase()
							+ "-" + logDate, type.toLowerCase())
						.setSource(toBeIndexed).execute();
				future.addListener(new ESIndexActionListener(tuple, collector, LOG));
				future.actionGet();
			} else {
				// Synchronous way
				IndexResponse response = client
						.prepareIndex(ES_INDEX_PREFIX + index.toLowerCase()
							+ "-" + logDate, type.toLowerCase())
						.setSource(toBeIndexed).get();
				if (response == null) {
					collector.reportError(new RuntimeException("ES null response"));
					collector.fail(tuple);
					LOG.error("Failed to index tuple due to ES null reponse: {} ", tuple.toString());
				} else {
					if (!response.getId().isEmpty()) {
						collector.ack(tuple);
						String documentId = response.getId();
						String logMsg = "Indexed successfully [" + index + "/"+ type + "/" + documentId + "]";
						// Anchored
						collector.emit(tuple, new Values(documentId));
						LOG.info(logMsg);
						LOG.debug("{} on tuple: {} ", logMsg, tuple.toString());
					} else {
						collector.reportError(new RuntimeException(response.toString()));
						collector.fail(tuple);
						LOG.error("Failed to index tuple: {} ", tuple.toString());
					}
				}
			}
		// We should try our best to handle all exceptions to ingest all logs.
		} catch (NodeClosedException nce) {
			nce.printStackTrace();
			collector.reportError(nce);
			collector.fail(tuple);
		} catch (ElasticsearchException ee) {
			// https://groups.google.com/forum/#!topic/storm-user/CGaKwFTa9TY
			ee.printStackTrace();
			collector.reportError(ee);
			collector.fail(tuple);
//			throw new ElasticsearchException("Unknown ElasticsearchException!");
		} catch (Exception e) {
			e.printStackTrace();
			collector.reportError(e);
			collector.fail(tuple);
//			throw new RuntimeException("Unknown Exception!");
//		} finally {
//			collector.ack(tuple);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("documentIndexId"));
	}

	@Override
	public void cleanup() {
		client.close();
	}

}