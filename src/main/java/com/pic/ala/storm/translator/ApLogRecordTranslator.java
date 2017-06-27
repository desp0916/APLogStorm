package com.pic.ala.storm.translator;

import static com.pic.ala.util.LogUtil.isNullOrEmpty;
import static com.pic.ala.util.LogUtil.parseDateTime;

import java.io.IOException;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.kafka.spout.RecordTranslator;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
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
	public static final String FIELD_APLOG = "apLog";
	public static final String FIELD_ES_SOURCE = "es_source";	// ElasticSearch 物件的 _source 欄位
	public static final String FIELD_SYS_ID = "sysID";			// 必填欄位
	public static final String FIELD_LOG_DATE = "logDate";		// 必填欄位
	public static final String FIELD_LOG_TYPE = "logType";		// 必填欄位
	public static final String FIELD_AP_ID = "apID";			// 必填欄位
	public static final String FIELD_AT = "reqAt";				// 必填欄位
	public static final String FIELD_MSG = "msg";				// 必填欄位

	@Override
	public List<Object> apply(ConsumerRecord<K, V> record) {

		String esSource = "";		// Log 原始資料（後續要插入 ES 的資料，對應到「_source」欄位）
		String sysID = "";			// 系統代碼，用於建立 ES 索引名稱的「系統代碼」部分
		String logDate = "";		// 日期，用於建立 ES 索引名稱的「日期」部分
		String logTime = "";
		String logType = "";
		String apID = "";
		String at = "";
		String msg = "";
		ApLog apLog = null;

		try {

			esSource = (String)record.value();
			ObjectMapper objectMapper = new ObjectMapper();
			apLog = objectMapper.readValue(esSource, ApLog.class);

			// 以下為 AP Log 的必填欄位
			sysID = apLog.getSysID();
			logType = apLog.getLogType();
			logTime = apLog.getLogTime();
			apID = apLog.getApID();
			at = apLog.getReqAt();
			msg = apLog.getMsg();

			String tmpLogDate = parseDateTime(logTime, FORMATS, FORMAT_DATE);

			if (!isNullOrEmpty(tmpLogDate)) {
				logDate = tmpLogDate;
				// 將 logTime 欄位強制轉為 ISO8601 格式的字串（yyyy-MM-dd'T'HH:mm:ss.SSSZZ）
				// apLog.setLogTime(toISO8601String(logTime));
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

//		return new Values(apLog, sysID, logType, logDate, apID, at, msg);
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
