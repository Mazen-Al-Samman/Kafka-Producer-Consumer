<?php

use RdKafka\Conf;
use RdKafka\Consumer;
use RdKafka\TopicConf;

$conf = new Conf();
$conf->set('bootstrap.servers', 'localhost:9092');
$conf->set('group.id', 'async_task_queue');
$conf->set('enable.auto.commit', 'true');

$consumer = new Consumer($conf);
$topicConf = new TopicConf();
$topicConf->set('auto.offset.reset', 'earliest');
$topic = $consumer->newTopic('second_topic', $topicConf);
$topic->consumeStart(0, RD_KAFKA_OFFSET_STORED);

//$consumer = new \RdKafka\KafkaConsumer($conf);
//$consumer->subscribe(['second_topic']);

while (true) {
    $message = $topic->consume(0, 120 * 1000);
    $messageArray = json_decode($message->payload, true);
    if (empty($messageArray)) continue;
    if (count($messageArray) < 2) throw new Exception("Invalid Number of Arguments");

    [$className, $function] = [array_shift($messageArray), array_shift($messageArray)];
    require_once __DIR__ . (str_replace("\\", "/", "/$className.php"));

    $classObject = new $className;
    call_user_func_array([$classObject, $function], []);

    if ($message->err === RD_KAFKA_RESP_ERR_NO_ERROR) continue;
    echo match ($message->err) {
        RD_KAFKA_RESP_ERR__PARTITION_EOF => "No more messages!" . PHP_EOL,
        RD_KAFKA_RESP_ERR__TIMED_OUT => "Consume Timed out!" . PHP_EOL,
        default => throw new \Exception($message->errstr(), $message->err),
    };
}