package com.pic.ala;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

// ES 1.7.4
//import org.elasticsearch.common.settings.ImmutableSettings;

public class ESConnTest {

	private static Client client;
	private static TransportClient transportClient;
	private static final boolean ES_SHIELD_ENABLED = true;

	public static void main(String[] args) {

		String esNodesString = "hdp01,hdp02,hdp03,hdp04,hdp05";
		List<String> esNodesList = Arrays.asList(esNodesString.split("\\s*,\\s*"));

		final Settings settings = Settings.builder()
				.put("cluster.name", "elasticsearch")
				.put("client.transport.sniff", true)
				.build();

		PreBuiltTransportClient preBuiltTransportClient = new PreBuiltTransportClient(settings);

		for (String esNode : esNodesList) {
			try {

				preBuiltTransportClient.addTransportAddress(
						new InetSocketTransportAddress(InetAddress.getByName(esNode), 9300));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		TransportClient transportClient = preBuiltTransportClient;

		for (DiscoveryNode dNode: transportClient.connectedNodes()) {
			System.out.println(dNode.toString());
		}

		client = transportClient;
		String indexName  = "aplog_aes3g-2016.04.12";
		String indexType = "ui";

		String toBeIndexed = "{\"sysID\":\"wds\",\"logType\":\""+indexType+"\",\"logTime\":\"2016-04-12T16:51:31.924+0800\",\"apID\":\"UIApp01V4\",\"functID\":\"FUNC_10002\",\"who\":\"機器人\",\"from\":\"iis\",\"at\":\"websphere\",\"to\":\"postgres\",\"action\":\"訂單成立\",\"result\":\"失敗\",\"kw\":\"玩命關頭\",\"msgLevel\":\"ERROR\",\"msg\":\"Unsufficient privilege\",\"msgCode\":\"5260\",\"table\":\"CODES\",\"dataCnt\":176,\"procTime\":80}";
		IndexResponse response = client
				.prepareIndex(indexName, indexType)
				.setSource(toBeIndexed).get();

		if (!response.getId().isEmpty()) {
			String documentIndexId = response.getId();
			// Anchored
			System.out.println("OK. The documentIndexId: " + documentIndexId);
		} else {
			System.out.println("FAILED");
		}

		client.close();
	}
}