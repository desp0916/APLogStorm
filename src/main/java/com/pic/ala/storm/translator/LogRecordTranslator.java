package com.pic.ala.storm.translator;

import static com.pic.ala.util.LogUtil.isNullOrEmpty;
import static com.pic.ala.util.LogUtil.parseDateTime;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.kafka.spout.RecordTranslator;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 *
 * Translate a {@link org.apache.kafka.clients.consumer.ConsumerRecord} to a tuple.
 *
 * @author Gary Liu <gary_liu@pic.net.tw>
 * @since  2017-04-17 14:03:02
 *
 * @param <K>
 * @param <V>
 */
public class LogRecordTranslator<K, V> implements RecordTranslator<K, V> {

	private static final long serialVersionUID = -7412940397600025976L;

	private static final Logger LOG = LoggerFactory.getLogger(LogRecordTranslator.class);

	public static final String FORMAT_DATE = "yyyy.MM.dd";

	private static final String[] FORMATS = new String[] {
		"yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
		"yyyy-MM-dd'T'HH:mm:ss.SSSZZ",
		"yyyy-MM-dd'T'HH:mm:ss.SSSZ",
		"yyyy-MM-dd HH:mm:ss.SSS",
		"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
	};

	public static final String FIELD_ES_SOURCE = "es_source";	// Elasticsearch "source" field
	public static final String FIELD_INDEX = "index";
	public static final String FIELD_TYPE = "type";
	public static final String FIELD_LOG_DATE = "logDate";
	public static final String FIELD_LOG_DATETIME = "logDateTime";
	public static final String FIELD_MESSAGE = "message";

	@Override
	public List<Object> apply(ConsumerRecord<K, V> record) {
		String esSource = "";
		String index = "";
		String type = "";
		String message = "";
		String logDate = "";

		try {
			esSource = (String)record.value();

			ObjectMapper objectMapper = new ObjectMapper();
			Map<String, String> logEntry = objectMapper.readValue(esSource, Map.class);

			index = logEntry.get(FIELD_INDEX);
			type = logEntry.get(FIELD_TYPE);
			message = logEntry.get(FIELD_MESSAGE);

			String tmpLogDate = parseDateTime(logEntry.get("@timestamp"), FORMATS, FORMAT_DATE);

			if (!isNullOrEmpty(tmpLogDate)) {
				logDate = tmpLogDate;
			}

		} catch (JsonParseException e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
		} catch (JsonMappingException e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
		}

		return new Values(esSource, index, type, logDate, message);
	}

	@Override
	public Fields getFieldsFor(String stream) {
		return new Fields(FIELD_ES_SOURCE, FIELD_INDEX, FIELD_TYPE,
				FIELD_LOG_DATE, FIELD_MESSAGE);
	}

	@Override
	public List<String> streams() {
		return DEFAULT_STREAM;
	}

}
