package com.pic.ala;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class ESearchTest {

	private static TransportClient transportClient;

	public static Client getClient() {
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

//		for (DiscoveryNode dNode : transportClient.connectedNodes()) {
//			System.out.println(dNode.toString());
//		}

		return transportClient;
	}

	// https://www.elastic.co/guide/en/elasticsearch/client/java-api/2.3/_structuring_aggregations.html
	public static void testAggregation1() {
		Client client = getClient();
		SearchResponse sr = client.prepareSearch("aplog_aes3g-2016.12")
				.addAggregation(
						AggregationBuilders.terms("by_functID").field("functID")
//							.subAggregation(AggregationBuilders.terms("by_result").field("dataCnt"))
						)
				.execute().actionGet();
		// NOTE: We need to call getHits() TWICE here!!!
		SearchHit[] results = sr.getHits().getHits();
		for (SearchHit hit : results) {
			String sourceAsString = hit.getSourceAsString();
			if (sourceAsString != null) {
				System.out.println(sourceAsString);
//				Gson gson = new GsonBuilder().setDateFormat("").create();
//				System.out.println(gson.fromJson(sourceAsString, Firewall.class));
			}
		}
	}

	// https://www.elastic.co/guide/en/elasticsearch/client/java-api/2.3/_metrics_aggregations.html#java-aggs-metrics-max
	public static void testMaxAggregation() {
		Client client = getClient();
		MinAggregationBuilder aggregation =
		        AggregationBuilders
		                .min("agg")
		                .field("height");
		SearchResponse sr = client.prepareSearch("aplog_aes3g-2016.12.15")
				.addAggregation(aggregation).execute().actionGet();
		Max agg = sr.getAggregations().get("agg");
		double value = agg.getValue();
		System.out.println(value);
	}

	public static void testSumAggregation() {
		Client client = getClient();
		SumAggregationBuilder aggregation = AggregationBuilders.sum("agg").field("dataCnt");
		SearchResponse sr = client.prepareSearch("aplog_aes3g-2016.12.15")
				.addAggregation(aggregation).execute().actionGet();
		Sum agg = sr.getAggregations().get("agg");
		double value = agg.getValue();
		System.out.println(value);
	}

	// Filter then count
	public static void testFilterAggregation() {
		Client client = getClient();
		FilterAggregationBuilder aggregation = AggregationBuilders.filter("agg", QueryBuilders.termQuery("logType", "batch"));
		SearchResponse sr = client.prepareSearch("aplog_aes3g-2016.12.15")
				.addAggregation(aggregation).execute().actionGet();
		Filter agg = sr.getAggregations().get("agg");

		long count = agg.getDocCount(); // Doc count
		System.out.println(count);
	}

	public static void main(String[] args) {
//		testMaxAggregation();
//		testSumAggregation();
		testFilterAggregation();
	}

}
