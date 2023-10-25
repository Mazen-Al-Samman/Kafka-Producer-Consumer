<?php

use RdKafka\Producer;
use RdKafka\TopicConf;

$conf = new RdKafka\Conf();
$conf->set('bootstrap.servers', 'localhost:9092');

$topicConfig = new TopicConf();
$producer = new Producer($conf);
$topic = $producer->newTopic('second_topic', $topicConfig);
$topic->produce(RD_KAFKA_PARTITION_UA, 0, json_encode([\Jobs\HelloWorld::class, 'helloWorld']));
$producer->poll(0);

for ($flushRetries = 0; $flushRetries < 10; $flushRetries++) {
    $result = $producer->flush(10000);
    if (RD_KAFKA_RESP_ERR_NO_ERROR === $result) break;
}

if (RD_KAFKA_RESP_ERR_NO_ERROR !== $result) throw new RuntimeException('Was unable to flush, messages might be lost!');