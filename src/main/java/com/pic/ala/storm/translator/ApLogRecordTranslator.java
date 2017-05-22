package com.pic.ala.storm.translator;

import static com.pic.ala.util.LogUtil.parseDateTime;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.kafka.spout.RecordTranslator;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pic.ala.model.ApLog;

/**
 *
 * Translate a {@link org.apache.kafka.clients.consumer.ConsumerRecord} to a tuple.
 *
 * @author Gary Liu <gary_liu@pic.net.tw>
 * @since  2017-04-17 10:41:11
 *
 * @param <K>
 * @param <V>
 */

public class ApLogRecordTranslator<K, V> implements RecordTranslator<K, V> {

	private static final long serialVersionUID = 517720175584770827L;

    private static final Logger LOG = LoggerFactory.getLogger(ApLogRecordTranslator.class);
	public static final String FORMAT_DATE = "yyyy.MM.dd";
	private static final String[] FORMATS = new String[] {
		"yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
		"yyyy-MM-dd'T'HH:mm:ss.SSSZZ",
		"yyyy-MM-dd'T'HH:mm:ss.SSSZ",
		"yyyy-MM-dd HH:mm:ss.SSS",
		"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
	};

	// The following fields will be used or stored by Elasticsearch.
	public static final String FIELD_ES_SOURCE = "es_source";	// ElasticSearch 物件的 source 欄位
	public static final String FIELD_SYS_ID = "sysID";
	public static final String FIELD_LOG_DATE = "logDate";
	public static final String FIELD_LOG_TYPE = "logType";
	public static final String FIELD_AP_ID = "apID";
	public static final String FIELD_FUNCT_ID = "functID";
	public static final String FIELD_WHO = "who";
	public static final String FIELD_FROM = "reqFrom";
	public static final String FIELD_AT = "reqAt";
	public static final String FIELD_TO = "reqTo";
	public static final String FIELD_ACTION = "reqAction";
	public static final String FIELD_RESULT = "reqResult";
	public static final String FIELD_KW = "kw";
	public static final String FIELD_MSG_LEVEL = "msgLevel";
	public static final String FIELD_MSG = "msg";
	public static final String FIELD_MSG_CODE = "msgCode";
	public static final String FIELD_TABLE = "reqTable";
	public static final String FIELD_DATA_CNT = "dataCnt";
	public static final String FIELD_PROC_TIME = "procTime";

	@Override
	public List<Object> apply(ConsumerRecord<K, V> record) {

		String esSource = "";
		String sysID = "";
		String logType = "";
		String apID = "";
		String at = "";
		String msg = "";
		String logDate = "";

		try {

			esSource = (String)record.value();
			ObjectMapper objectMapper = new ObjectMapper();
			ApLog apLog = objectMapper.readValue(esSource, ApLog.class);

			sysID = apLog.getSysID();
			logType = apLog.getLogType();
			apID = apLog.getApID();
			at = apLog.getReqAt();
			msg = apLog.getMsg();
			String tmpLogDate = parseDateTime(apLog.getLogTime(), FORMATS, FORMAT_DATE);

			if (tmpLogDate != null) {
				logDate = tmpLogDate;
			}

		} catch (Exception e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
		}

		return new Values(esSource, sysID, logType, logDate, apID, at, msg);
	}

	@Override
	public Fields getFieldsFor(String stream) {
		return new Fields(FIELD_ES_SOURCE, FIELD_SYS_ID, FIELD_LOG_TYPE,
				FIELD_LOG_DATE, FIELD_AP_ID, FIELD_AT, FIELD_MSG);
	}

	@Override
	public List<String> streams() {
		return DEFAULT_STREAM;
	}

}
