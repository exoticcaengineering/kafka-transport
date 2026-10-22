<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Transport;

use Exception;
use Exoticca\KafkaMessenger\Transport\Serializer\MessageSerializer;
use Exoticca\KafkaMessenger\Transport\Setting\CommitMode;
use Exoticca\KafkaMessenger\Transport\Setting\GeneralSetting;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\KafkaConsumerTopic;
use RdKafka\Message;
use RdKafka\Producer;
use Symfony\Component\Console\SignalRegistry\SignalRegistry;
use Symfony\Component\Messenger\Exception\TransportException;

class KafkaConnection
{
    private bool $consumerSubscribed = false;
    private bool $consumerMustBeRunning = false;
    private const WILDCARD_ROUTING = '*';
    // The consumer no longer owns the partition, so whoever owns it now reads the message.
    private const PARTITION_LOST_ERRORS = [
        RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION,
        RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID,
        RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS,
        RD_KAFKA_RESP_ERR__ASSIGNMENT_LOST,
    ];
    // The partition was revoked while the message was handled, so whoever owns it now reads the message again.
    private const OFFSET_STORE_PARTITION_LOST_ERRORS = [
        RD_KAFKA_RESP_ERR__STATE,
        RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION,
    ];
    private ?KafkaConsumer $consumer;
    /** @var array<string, KafkaConsumerTopic> */
    private array $consumerTopics = [];
    private ?Producer $producer;

    private SignalRegistry $signalRegistry;
    private KafkaDeliveryReports $deliveryReports;

    public function __construct(
        private readonly GeneralSetting      $generalSetting,
    ) {
        $this->signalRegistry = new SignalRegistry();
        $this->deliveryReports = new KafkaDeliveryReports();
    }


    public function get(
        array $topicsToFilter,
    ): ?iterable {
        $consumer = $this->getConsumer();

        if (!$this->consumerSubscribed) {
            $consumer->subscribe(!empty($topicsToFilter) ? $topicsToFilter : $this->generalSetting->consumer->topics);
            $this->consumerSubscribed = true;
        }
        $this->consumerMustBeRunning = true;

        yield from $this->doReceive(
            timeout: $this->generalSetting->consumer->consumeTimeout ?? 500
        );
    }

    private function doReceive(
        int $timeout,
    ): iterable {
        $this->signalRegistry->register(SIGINT, fn () => $this->consumerMustBeRunning = false);
        $this->signalRegistry->register(SIGTERM, fn () => $this->consumerMustBeRunning = false);

        while ($this->consumerMustBeRunning) {
            $kafkaMessage = $this->getConsumer()->consume($timeout);

            if (null === $kafkaMessage) {
                yield null;
            }

            switch ($kafkaMessage->err) {
                case \RD_KAFKA_RESP_ERR_NO_ERROR:
                    $messageIdentifier = $kafkaMessage->headers[MessageSerializer::identifierHeaderKey()] ?? null;
                    $forceAckByRoutingMap = false;

                    $messageFoundInRouting = false;
                    foreach ($this->generalSetting->consumer->routing as $name => $class) {
                        if ($name == self::WILDCARD_ROUTING) {
                            $messageFoundInRouting = true;
                            break;
                        }

                        if (is_null($messageIdentifier)) {
                            break;
                        }

                        if ($name == $messageIdentifier) {
                            $messageFoundInRouting = true;
                            break;
                        }
                    }

                    if (!$messageFoundInRouting) {
                        $forceAckByRoutingMap = true;
                    }

                    if ($forceAckByRoutingMap) {
                        $this->commit($kafkaMessage);
                        break;
                    }


                    if ($this->generalSetting->consumer->commitOnReceive) {
                        // Only the consumer that committed the message handles it. If the partition was revoked
                        // before the commit, another consumer gets the message.
                        if ($this->commitAndLeave($kafkaMessage)) {
                            yield $kafkaMessage;
                        }

                        return;
                    }

                    yield $kafkaMessage;
                    // no break
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                case RD_KAFKA_RESP_ERR__TRANSPORT:
                case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART:
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    yield null;
                    // no break
                default:
                    throw new \LogicException($kafkaMessage->errstr(), $kafkaMessage->err);
                    break;
            }
        }
    }


    public function ack(Message $message): void
    {
        // Already committed on receive, and this consumer left the group.
        if ($this->generalSetting->consumer->commitOnReceive) {
            return;
        }

        $this->commit($message);
    }

    private function commit(Message $message): void
    {
        match ($this->generalSetting->consumer->commitMode) {
            CommitMode::SYNC => $this->getConsumer()->commit($message),
            CommitMode::ASYNC => $this->getConsumer()->commitAsync($message),
            CommitMode::STORE => $this->storeOffset($message),
        };
    }

    /**
     * Only marks the message as done: librdkafka commits the stored offsets every auto.commit.interval.ms
     * and when partitions are revoked, so skipping a burst of messages doesn't send a commit for each one.
     */
    private function storeOffset(Message $message): void
    {
        $topic = $this->consumerTopics[$message->topic_name] ??= $this->getConsumer()->newTopic($message->topic_name);

        try {
            // rd_kafka_offset_store() commits offset + 1, the next message to read
            $topic->offsetStore($message->partition, $message->offset);
        } catch (\RdKafka\Exception $e) {
            if (\in_array($e->getCode(), self::OFFSET_STORE_PARTITION_LOST_ERRORS, true)) {
                return;
            }

            throw $e;
        }
    }

    /**
     * Commits the message and leaves the group before it's handled, so a slow handler never holds
     * partitions or stalls a rebalance. Delivery becomes at-most-once: a worker killed while handling
     * loses the message. The next get() joins the group again.
     */
    private function commitAndLeave(Message $message): bool
    {
        try {
            $this->getConsumer()->commit($message);

            return true;
        } catch (\RdKafka\Exception $e) {
            if (\in_array($e->getCode(), self::PARTITION_LOST_ERRORS, true)) {
                return false;
            }

            throw $e;
        } finally {
            $this->consumer->close();
            $this->consumer = null;
            $this->consumerTopics = [];
            $this->consumerSubscribed = false;
        }
    }

    /**
     * @param array<string, string> $headers
     * @throws \RdKafka\Exception
     */
    public function produce(
        int      $partition,
        int      $messageFlags,
        string   $body,
        ?string  $key = null,
        array    $headers = [],
        bool     $forceFlush = true,
        ?string  $identifier = null,
        ?callable $beforeProduceConvertBody = null,
    ): void {
        $producer = $this->getProducer();
        $topicFromRouting = null !== $identifier
            ? $this->generalSetting->producer->routing[$identifier] ?? null
            : null;

        foreach ($this->generalSetting->producer->topics as $topic) {
            if ($topicFromRouting && $topic != $topicFromRouting) {
                continue;
            }

            if ($beforeProduceConvertBody) {
                $body = $beforeProduceConvertBody($topic);
            }

            $topic = $producer->newTopic($topic);
            $topic->producev(
                $partition,
                $messageFlags,
                $body,
                $key,
                $headers,
            );

            $producer->poll($this->generalSetting->producer->pollTimeoutMs);
        }

        if ($forceFlush) {
            $this->flush();
        }
    }

    /**
     * Sends the raw message to the configured DLQ topic. Returns the topic, or null when no DLQ is configured.
     *
     * @throws TransportException when the topic doesn't exist or the message can't be flushed
     */
    public function produceToDlq(Message $message, \Throwable $error): ?string
    {
        $dlqTopic = $this->generalSetting->consumer->dlqTopic;
        if (null === $dlqTopic) {
            return null;
        }

        $producer = $this->getProducer();
        $topic = $producer->newTopic($dlqTopic);

        // Fail before producing, so a missing DLQ topic is reported as such and not as a failed delivery.
        // ponytail: one metadata request per undecodable message, cache it if bad messages ever get frequent.
        foreach ($producer->getMetadata(false, $topic, $this->generalSetting->producer->flushTimeoutMs)->getTopics() as $metadataTopic) {
            if (RD_KAFKA_RESP_ERR_NO_ERROR !== $metadataTopic->getErr()) {
                throw new TransportException(sprintf('DLQ topic "%s" does not exist', $dlqTopic), $metadataTopic->getErr());
            }
        }

        $topic->producev(
            RD_KAFKA_PARTITION_UA,
            0,
            $message->payload,
            $message->key,
            [
                ...($message->headers ?? []),
                'x-dlq-error' => $error->getMessage(),
                'x-dlq-original-topic' => (string) $message->topic_name,
                'x-dlq-original-partition' => (string) $message->partition,
                'x-dlq-original-offset' => (string) $message->offset,
            ],
        );
        $this->flush();

        return $dlqTopic;
    }

    /**
     * Also throws when a message produced since the last flush was rejected by the broker,
     * including the ones sent with KafkaNoFlushStamp.
     *
     * @throws TransportException
     */
    public function flush(): void
    {
        for ($flushRetries = 0; $flushRetries < 10; ++$flushRetries) {
            $result = $this->getProducer()->flush($this->generalSetting->producer->flushTimeoutMs);

            if (RD_KAFKA_RESP_ERR_NO_ERROR === $result) {
                break;
            }
        }

        if (RD_KAFKA_RESP_ERR_NO_ERROR !== $result) {
            throw new TransportException('Was unable to flush, messages might be lost!: '.$result, $result);
        }

        $this->deliveryReports->throwIfAnyFailed();
    }

    private function getConsumer(): KafkaConsumer
    {
        return $this->consumer ??= $this->createConsumer($this->generalSetting->consumer->config);
    }

    private function getProducer(): Producer
    {
        return $this->producer ??= $this->createProducer($this->generalSetting->producer->config);
    }

    private function getBaseConf(): Conf
    {
        $conf = new Conf();
        $conf->set('metadata.broker.list', $this->generalSetting->host);
        return $conf;
    }

    private function createConsumer(array $kafkaConfig): KafkaConsumer
    {
        $conf = $this->getBaseConf();
        // php-rdkafka 6.0 has no getRebalanceProtocol(), so take the protocol from the configured strategy
        $cooperative = str_contains((string) ($kafkaConfig['partition.assignment.strategy'] ?? ''), 'cooperative');
        $conf->setRebalanceCb(
            fn (KafkaConsumer $kafka, $err, ?array $partitions = null) => $this->rebalance($kafka, $err, $partitions, $cooperative),
        );

        foreach ($kafkaConfig as $key => $value) {
            $conf->set($key, $value);
        }

        return new KafkaConsumer($conf);
    }

    /**
     * Eager protocols revoke and reassign every partition on each rebalance. Cooperative ones only
     * hand over the partitions that move, so the rest keep being consumed during the rebalance.
     */
    private function rebalance(KafkaConsumer $kafka, int $err, ?array $partitions, bool $cooperative): void
    {
        switch ($err) {
            case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                $cooperative ? $kafka->incrementalAssign($partitions) : $kafka->assign($partitions);

                break;

            case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                $cooperative ? $kafka->incrementalUnassign($partitions) : $kafka->assign(null);

                break;

            default:
                throw new Exception($err);
        }
    }

    /**
     * @param array<string, string> $kafkaConfig
     */
    private function createProducer(array $kafkaConfig): Producer
    {
        $conf = $this->getBaseConf();
        $conf->setDrMsgCb(fn (Producer $producer, Message $message) => $this->deliveryReports->record($message));

        foreach ($kafkaConfig as $key => $value) {
            $conf->set($key, $value);
        }

        return new Producer($conf);
    }
}
