<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Tests\Unit\Transport;

use Exoticca\KafkaMessenger\Transport\KafkaDeliveryReports;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use RdKafka\Message;
use Symfony\Component\Messenger\Exception\TransportException;

#[CoversClass(KafkaDeliveryReports::class)]
class KafkaDeliveryReportsTest extends TestCase
{
    public function test_successful_deliveries_do_not_throw(): void
    {
        $reports = new KafkaDeliveryReports();
        $reports->record(self::deliveryReport(RD_KAFKA_RESP_ERR_NO_ERROR, 'test-topic'));

        $reports->throwIfAnyFailed();

        $this->expectNotToPerformAssertions();
    }

    public function test_failed_delivery_throws_with_error_and_topic(): void
    {
        $reports = new KafkaDeliveryReports();
        $reports->record(self::deliveryReport(RD_KAFKA_RESP_ERR_NO_ERROR, 'test-topic'));
        $reports->record(self::deliveryReport(RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART, 'missing-topic'));

        try {
            $reports->throwIfAnyFailed();
            $this->fail('Expected a TransportException');
        } catch (TransportException $e) {
            $this->assertSame(RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART, $e->getCode());
            $this->assertSame(
                sprintf('Kafka did not deliver 1 message(s): %s (topic "missing-topic")', rd_kafka_err2str(RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART)),
                $e->getMessage(),
            );
        }
    }

    public function test_failures_are_reported_only_once(): void
    {
        $reports = new KafkaDeliveryReports();
        $reports->record(self::deliveryReport(RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE, 'test-topic'));

        try {
            $reports->throwIfAnyFailed();
        } catch (TransportException) {
        }

        $reports->throwIfAnyFailed();
        $this->expectNotToPerformAssertions();
    }

    private static function deliveryReport(int $err, string $topic): Message
    {
        $message = new Message();
        $message->err = $err;
        $message->topic_name = $topic;

        return $message;
    }
}
