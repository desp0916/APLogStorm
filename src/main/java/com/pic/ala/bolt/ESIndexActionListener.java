/**
 * Index the log stream in asynchronous way.
 *
 * Ref:
 *
 * http://stackoverflow.com/questions/30234612/storm-kafkaspout-fails-when-bolt-is-slow
 *
 * Storm will fail a tuple if it takes too long to process, by default 30 seconds.
 * Since Storm guarantees processing, once failed the Kafka spout will replay the same
 * message *until the tuple is successfully processed*.
 *
 */
package com.pic.ala.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.slf4j.Logger;

public class ESIndexActionListener implements ActionListener<IndexResponse> {

	private final Tuple tuple;
	private final OutputCollector collector;
	private final Logger logger;

	public ESIndexActionListener(Tuple tuple, OutputCollector collector, Logger logger) {
		super();
		this.tuple = tuple;
		this.collector = collector;
		this.logger = logger;
	}

	@Override
	public void onResponse(IndexResponse response) {
		if (!response.getId().isEmpty()) {
			collector.ack(tuple);
			String index = response.getIndex();
			String type = response.getType();
			String documentId = response.getId();
			String logMsg = "Indexed successfully [" + index + "/"+ type + "/" + documentId + "]";
			// Anchored
			collector.emit(tuple, new Values(documentId));
			logger.info(logMsg);
			logger.debug("{} on tuple: {} ", logMsg, tuple.toString());
		} else {
			collector.reportError(new Throwable(response.toString()));
			collector.fail(tuple);
			logger.error("Failed to index tuple asynchronously: {} ", tuple.toString());
		}
	}

	@Override
	public void onFailure(Exception e) {
		collector.reportError(e);
		collector.fail(tuple);
		logger.error("Index failure on tuple asynchronously: {} ", tuple.toString());
	}

}