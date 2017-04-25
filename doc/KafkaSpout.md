# KafkaSpout

由於 Storm 1.10.0 和 Kafka 0.10 以後，兩者皆有許多重大改變，所以 Storm 社群也重寫了 KafkaSpout，並建議使用這兩個版本的 users 都改用 [storm-kafka-client](https://github.com/apache/storm/tree/master/external/storm-kafka-client)。


其中最容易遇到的一個問題就是在 Storm UI 上可以看到由 KafkaSpout 吐出來的警告：

```
org.apache.kafka.clients.consumer.CommitFailedException: Commit cannot be completed since the group has already rebalanced and assigned the partitions to another member. This means that the time between subsequent calls to poll() was longer than the configured max.poll.interval.ms, which typically implies that the poll loop is spending too much time message processing. You can address this either by increasing the session timeout or by reducing the maximum size of batches returned in poll() with max.poll.records. at org.apache.kafka.clients.consumer.internals.ConsumerCoordinator$OffsetCommitResponseHandler.handle(ConsumerCoordinator.java:766) at org.apache.kafka.clients.consumer.internals.ConsumerCoordinator$OffsetCommitResponseHandler.handle(ConsumerCoordinator.java:712) at org.apache.kafka.clients.consumer.internals.AbstractCoordinator$CoordinatorResponseHandler.onSuccess(AbstractCoordinator.java:764) at org.apache.kafka.clients.consumer.internals.AbstractCoordinator$CoordinatorResponseHandler.onSuccess(AbstractCoordinator.java:745) at org.apache.kafka.clients.consumer.internals.RequestFuture$1.onSuccess(RequestFuture.java:186) at org.apache.kafka.clients.consumer.internals.RequestFuture.fireSuccess(RequestFuture.java:149) at org.apache.kafka.clients.consumer.internals.RequestFuture.complete(RequestFuture.java:116) at org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient$RequestFutureCompletionHandler.fireCompletion(ConsumerNetworkClient.java:493) at org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient.firePendingCompletedRequests(ConsumerNetworkClient.java:322) at org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient.poll(ConsumerNetworkClient.java:253) at org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient.poll(ConsumerNetworkClient.java:188) at org.apache.kafka.clients.consumer.internals.ConsumerCoordinator.commitOffsetsSync(ConsumerCoordinator.java:578) at org.apache.kafka.clients.consumer.KafkaConsumer.commitSync(KafkaConsumer.java:1125) at org.apache.storm.kafka.spout.KafkaSpout.commitOffsetsForAckedTuples(KafkaSpout.java:384) at org.apache.storm.kafka.spout.KafkaSpout.nextTuple(KafkaSpout.java:219) at org.apache.storm.daemon.executor$fn__10363$fn__10378$fn__10409.invoke(executor.clj:645) at org.apache.storm.util$async_loop$fn__553.invoke(util.clj:484) at clojure.lang.AFn.run(AFn.java:22) at java.lang.Thread.run(Thread.java:745)
```

經過不斷地爬文與調整參數，


## 參考資料：

  1. [Apache Storm Component Guide - ​Tuning KafkaSpout Performance](https://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.6.0/bk_storm-component-guide/content/storm-kafkaspout-perf.html)
  2. [Difference between session.timeout.ms and max.poll.interval.ms for Kafka 0.10.0.0 and later versions](http://stackoverflow.com/questions/39730126/difference-between-session-timeout-ms-and-max-poll-interval-ms-for-kafka-0-10-0)
  3. [Kafka - Notable changes in 0.10.1.0](https://kafka.apache.org/documentation/)
  4. [kafka容错对消费者影响分析](http://kaimingwan.com/post/kafka/kafkarong-cuo-dui-xiao-fei-zhe-ying-xiang-fen-xi)

```
/usr/hdp/current/storm-client/bin/storm-kafka-monitor -b hdp01:6667,hdp02:6667,hdp03:6667,hdp04:6667,hdp05:6667 -g aplog-analyzer -t ap-log-v1 | prettyjson

log4j:WARN No appenders could be found for logger (org.apache.kafka.shaded.clients.consumer.ConsumerConfig).
log4j:WARN Please initialize the log4j system properly.
log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.
{
    "ap-log-v1": {
        "0": {
            "consumerCommittedOffset": 35167,
            "lag": 1,
            "logHeadOffset": 35168
        },
        "1": {
            "consumerCommittedOffset": 35267,
            "lag": 2,
            "logHeadOffset": 35269
        },
        "2": {
            "consumerCommittedOffset": 35204,
            "lag": 2,
            "logHeadOffset": 35206
        },
        "3": {
            "consumerCommittedOffset": 35116,
            "lag": 1,
            "logHeadOffset": 35117
        },
        "4": {
            "consumerCommittedOffset": 35204,
            "lag": 2,
            "logHeadOffset": 35206
        },
        "5": {
            "consumerCommittedOffset": 35175,
            "lag": 2,
            "logHeadOffset": 35177
        },
        "6": {
            "consumerCommittedOffset": 35157,
            "lag": 1,
            "logHeadOffset": 35158
        },
        "7": {
            "consumerCommittedOffset": 35203,
            "lag": 2,
            "logHeadOffset": 35205
        },
        "8": {
            "consumerCommittedOffset": 35231,
            "lag": 1,
            "logHeadOffset": 35232
        },
        "9": {
            "consumerCommittedOffset": 35201,
            "lag": 1,
            "logHeadOffset": 35202
        }
    }
}

```
