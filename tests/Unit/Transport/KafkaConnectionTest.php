<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Tests\Unit\Transport;

use Exoticca\KafkaMessenger\Tests\ObjectMother\GeneralSettingMother;
use Exoticca\KafkaMessenger\Transport\KafkaConnection;
use Exoticca\KafkaMessenger\Transport\KafkaDeliveryReports;
use Exoticca\KafkaMessenger\Transport\Serializer\MessageSerializer;
use Exoticca\KafkaMessenger\Transport\Setting\ConsumerSetting;
use Exoticca\KafkaMessenger\Transport\Setting\GeneralSetting;
use Exoticca\KafkaMessenger\Transport\Setting\ProducerSetting;
use PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use RdKafka\KafkaConsumer;
use RdKafka\Message;
use RdKafka\Producer;
use RdKafka\ProducerTopic;
use RdKafka\TopicPartition;
use ReflectionClass;
use ReflectionMethod;
use Symfony\Component\Messenger\Exception\TransportException;

#[CoversClass(KafkaConnection::class)]
#[CoversClass(KafkaDeliveryReports::class)]
#[CoversClass(ConsumerSetting::class)]
#[CoversClass(ProducerSetting::class)]
#[CoversClass(GeneralSetting::class)]
#[CoversClass(MessageSerializer::class)]
#[AllowMockObjectsWithoutExpectations]
class KafkaConnectionTest extends TestCase
{
    private KafkaConnection $connection;
    private KafkaConsumer $consumer;
    private Producer $producer;
    private ProducerTopic $producerTopic;

    protected function setUp(): void
    {
        $this->consumer = $this->createMock(KafkaConsumer::class);
        $this->producer = $this->createMock(Producer::class);
        $this->producerTopic = $this->createMock(ProducerTopic::class);

        $generalSetting = GeneralSettingMother::create();
        $this->connection = new KafkaConnection($generalSetting);
    }

    private function stopConsumerLoop(): void
    {
        $this->setPropertyValue($this->connection, 'consumerMustBeRunning', false);
    }

    private function setPropertyValue(object $object, string $propertyName, mixed $value): void
    {
        $reflection = new ReflectionClass($object);
        $property = $reflection->getProperty($propertyName);
        $property->setValue($object, $value);
    }

    public function test_produce_sends_message_to_configured_topics(): void
    {
        $this->producer->expects($this->exactly(2))
            ->method('newTopic')
            ->willReturn($this->producerTopic);

        $this->producer->expects($this->exactly(2))
            ->method('poll')
            ->with(0);

        $this->producer->expects($this->once())
            ->method('flush')
            ->with(10000)
            ->willReturn(\RD_KAFKA_RESP_ERR_NO_ERROR);

        $this->producerTopic->method('producev')
            ->with(
                0,
                0,
                'test-body',
                'test-key',
                ['header1' => 'value1']
            );

        $this->injectMockProducer();

        $this->connection->produce(
            0,
            0,
            'test-body',
            'test-key',
            ['header1' => 'value1']
        );
    }

    public function test_produce_with_identifier_sends_to_specific_topic(): void
    {
        $this->producer->expects($this->once())
            ->method('newTopic')
            ->with('test-topic-1')
            ->willReturn($this->producerTopic);

        $this->producer->expects($this->once())
            ->method('poll')
            ->with(0);

        $this->producer->expects($this->once())
            ->method('flush')
            ->with(10000)
            ->willReturn(\RD_KAFKA_RESP_ERR_NO_ERROR);

        $this->producerTopic->method('producev')
            ->with(
                0,
                0,
                'test-body',
                'test-key',
                ['header1' => 'value1']
            );

        $this->injectMockProducer();

        $this->connection->produce(
            0,
            0,
            'test-body',
            'test-key',
            ['header1' => 'value1'],
            true,
            'test.message'
        );
    }

    public function test_produce_with_body_transformer(): void
    {
        $topic1 = $this->createMock(ProducerTopic::class);
        $topic2 = $this->createMock(ProducerTopic::class);

        $this->producer->expects($this->exactly(2))
            ->method('newTopic')
            ->willReturnCallback(function ($topicName) use ($topic1, $topic2) {
                if ($topicName === 'test-topic-1') {
                    return $topic1;
                }
                if ($topicName === 'test-topic-2') {
                    return $topic2;
                }
                return null;
            });

        $this->producer->expects($this->exactly(2))
            ->method('poll')
            ->with(0);

        $this->producer->expects($this->once())
            ->method('flush')
            ->with(10000)
            ->willReturn(\RD_KAFKA_RESP_ERR_NO_ERROR);

        $topic1->method('producev')
            ->with(
                0,
                0,
                'transformed-for-test-topic-1',
                'test-key',
                ['header1' => 'value1']
            );

        $topic2->method('producev')
            ->with(
                0,
                0,
                'transformed-for-test-topic-2',
                'test-key',
                ['header1' => 'value1']
            );

        $this->injectMockProducer();

        $this->connection->produce(
            0,
            0,
            'test-body',
            'test-key',
            ['header1' => 'value1'],
            true,
            null,
            function ($topic) {
                return "transformed-for-$topic";
            }
        );
    }

    public function test_flush_throws_exception_after_retries(): void
    {
        $this->producer->expects($this->exactly(10))
            ->method('flush')
            ->with(10000)
            ->willReturn(\RD_KAFKA_RESP_ERR__TIMED_OUT);

        $this->injectMockProducer();

        $this->expectException(TransportException::class);

        $this->connection->flush();
    }

    public function test_produce_does_not_throw_when_delivery_succeeds(): void
    {
        $this->producer->method('newTopic')->willReturn($this->producerTopic);
        $this->producer->method('poll')->willReturnCallback(function () {
            $this->deliveryReport(\RD_KAFKA_RESP_ERR_NO_ERROR, 'test-topic-1');

            return 1;
        });
        $this->producer->expects($this->once())
            ->method('flush')
            ->willReturn(\RD_KAFKA_RESP_ERR_NO_ERROR);

        $this->injectMockProducer();

        $this->connection->produce(0, 0, 'test-body', identifier: 'test.message');
    }

    public function test_produce_throws_when_broker_rejects_the_message(): void
    {
        $this->producer->method('newTopic')->willReturn($this->producerTopic);
        // flush() drains the queue fine, the error is only in the delivery report
        $this->producer->expects($this->once())
            ->method('flush')
            ->willReturnCallback(function () {
                $this->deliveryReport(\RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART, 'test-topic-1');

                return \RD_KAFKA_RESP_ERR_NO_ERROR;
            });

        $this->injectMockProducer();

        $this->expectException(TransportException::class);
        $this->expectExceptionCode(\RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART);
        $this->expectExceptionMessage('topic "test-topic-1"');

        $this->connection->produce(0, 0, 'test-body', identifier: 'test.message');
    }

    public function test_no_flush_delivery_errors_are_reported_on_next_flush(): void
    {
        $this->producer->method('newTopic')->willReturn($this->producerTopic);
        $this->producer->method('poll')->willReturnCallback(function () {
            $this->deliveryReport(\RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED, 'test-topic-1');

            return 1;
        });
        $this->producer->expects($this->once())
            ->method('flush')
            ->willReturn(\RD_KAFKA_RESP_ERR_NO_ERROR);

        $this->injectMockProducer();

        $this->connection->produce(0, 0, 'test-body', forceFlush: false, identifier: 'test.message');

        $this->expectException(TransportException::class);
        $this->expectExceptionCode(\RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED);

        $this->connection->flush();
    }

    public function test_get_subscribes_to_topics_and_returns_messages(): void
    {
        $message = $this->createConfiguredMessage(
            \RD_KAFKA_RESP_ERR_NO_ERROR,
            [MessageSerializer::identifierHeaderKey() => 'test.message']
        );

        $this->consumer->expects($this->once())
            ->method('subscribe')
            ->with(['test-topic-1', 'test-topic-2']);

        $this->consumer->method('consume')
            ->willReturn($message);

        $this->injectMockConsumer();

        $generator = $this->connection->get([]);

        $result = $generator->current();
        $this->assertSame($message, $result);

        $this->stopConsumerLoop();
    }

    public function test_get_with_topic_filter(): void
    {
        $this->consumer->expects($this->once())
            ->method('subscribe')
            ->with(['filtered-topic']);

        $emptyMessage = $this->createConfiguredMessage(\RD_KAFKA_RESP_ERR__PARTITION_EOF, []);

        $this->consumer->method('consume')
            ->willReturn($emptyMessage);

        $this->injectMockConsumer();
        $this->stopConsumerLoop();

        $generator = $this->connection->get(['filtered-topic']);
        $this->assertNull($generator->current());
    }

    public function test_message_not_in_routing_is_acknowledged(): void
    {
        $message = $this->createConfiguredMessage(
            \RD_KAFKA_RESP_ERR_NO_ERROR,
            [MessageSerializer::identifierHeaderKey() => 'unknown.message']
        );

        $emptyMessage = $this->createConfiguredMessage(\RD_KAFKA_RESP_ERR__PARTITION_EOF, []);

        $this->consumer->expects($this->once())
            ->method('subscribe')
            ->with(['test-topic-1', 'test-topic-2']);

        $this->consumer->expects($this->exactly(2))
            ->method('consume')
            ->willReturnOnConsecutiveCalls($message, $emptyMessage);

        $this->consumer->expects($this->exactly(1))
            ->method('commitAsync')
            ->with($this->identicalTo($message));

        $this->injectMockConsumer();

        $this->stopConsumerLoop();

        $generator = $this->connection->get([]);
        $generator->current();
    }

    public function test_commit_on_receive_commits_and_leaves_the_group_before_handling(): void
    {
        $this->connection = new KafkaConnection(GeneralSettingMother::create(['consumer' => ['commitOnReceive' => true]]));
        $message = $this->createConfiguredMessage(
            \RD_KAFKA_RESP_ERR_NO_ERROR,
            [MessageSerializer::identifierHeaderKey() => 'test.message']
        );

        $this->consumer->method('consume')->willReturn($message);
        $this->consumer->expects($this->once())->method('commit')->with($this->identicalTo($message));
        $this->consumer->expects($this->never())->method('commitAsync');
        $this->consumer->expects($this->once())->method('close');
        $this->injectMockConsumer();

        $generator = $this->connection->get([]);

        $this->assertSame($message, $generator->current());
        // Already out of the group while the message is handled.
        $this->assertNull((new ReflectionClass($this->connection))->getProperty('consumer')->getValue($this->connection));
        $generator->next();
        $this->assertFalse($generator->valid());

        // Handled outside the group: acking must not create a consumer and join again.
        $this->connection->ack($message);
        $this->assertNull((new ReflectionClass($this->connection))->getProperty('consumer')->getValue($this->connection));
    }

    public function test_commit_on_receive_still_commits_messages_not_in_routing(): void
    {
        $this->connection = new KafkaConnection(GeneralSettingMother::create(['consumer' => ['commitOnReceive' => true]]));
        $message = $this->createConfiguredMessage(
            \RD_KAFKA_RESP_ERR_NO_ERROR,
            [MessageSerializer::identifierHeaderKey() => 'unknown.message']
        );
        $emptyMessage = $this->createConfiguredMessage(\RD_KAFKA_RESP_ERR__PARTITION_EOF, []);

        $this->consumer->method('consume')->willReturnOnConsecutiveCalls($message, $emptyMessage);
        $this->consumer->expects($this->once())->method('commitAsync')->with($this->identicalTo($message));
        $this->consumer->expects($this->never())->method('close');
        $this->injectMockConsumer();
        $this->stopConsumerLoop();

        $this->connection->get([])->current();
    }

    public function test_commit_on_receive_skips_the_message_when_the_partition_was_revoked(): void
    {
        $this->connection = new KafkaConnection(GeneralSettingMother::create(['consumer' => ['commitOnReceive' => true]]));
        $message = $this->createConfiguredMessage(
            \RD_KAFKA_RESP_ERR_NO_ERROR,
            [MessageSerializer::identifierHeaderKey() => 'test.message']
        );

        $this->consumer->method('consume')->willReturn($message);
        $this->consumer->method('commit')->willThrowException(
            new \RdKafka\Exception('Broker: Specified group generation id is not valid', \RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION)
        );
        $this->consumer->expects($this->once())->method('close');
        $this->injectMockConsumer();

        $generator = $this->connection->get([]);

        $this->assertFalse($generator->valid());
    }

    public function test_commit_on_receive_throws_other_commit_errors(): void
    {
        $this->connection = new KafkaConnection(GeneralSettingMother::create(['consumer' => ['commitOnReceive' => true]]));
        $message = $this->createConfiguredMessage(
            \RD_KAFKA_RESP_ERR_NO_ERROR,
            [MessageSerializer::identifierHeaderKey() => 'test.message']
        );

        $this->consumer->method('consume')->willReturn($message);
        $this->consumer->method('commit')->willThrowException(
            new \RdKafka\Exception('Local: Timed out', \RD_KAFKA_RESP_ERR__TIMED_OUT)
        );
        $this->consumer->expects($this->once())->method('close');
        $this->injectMockConsumer();

        $this->expectException(\RdKafka\Exception::class);
        $this->connection->get([])->current();
    }

    public function test_ack_commits_message(): void
    {
        $message = new Message();

        $this->consumer->expects($this->once())
            ->method('commitAsync')
            ->with($message);

        $this->injectMockConsumer();

        $this->connection->ack($message);
    }

    public function test_ack_uses_commit_when_commit_async_is_true(): void
    {
        $generalSetting = GeneralSettingMother::create([
            'consumer' => [
                'commitAsync' => true
            ]
        ]);

        $this->connection = new KafkaConnection($generalSetting);

        $message = new Message();

        $this->consumer->expects($this->once())
            ->method('commitAsync')
            ->with($message);

        $this->injectMockConsumer();

        $this->connection->ack($message);
    }

    public function test_eager_rebalance_reassigns_all_partitions(): void
    {
        $partitions = [new TopicPartition('test_topic', 0), new TopicPartition('test_topic', 1)];

        $this->consumer->expects($this->exactly(2))
            ->method('assign')
            ->with($this->callback(fn (?array $assigned) => in_array($assigned, [$partitions, null], true)));
        $this->consumer->expects($this->never())->method('incrementalAssign');
        $this->consumer->expects($this->never())->method('incrementalUnassign');

        $this->rebalance(RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS, $partitions, cooperative: false);
        $this->rebalance(RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS, $partitions, cooperative: false);
    }

    public function test_cooperative_rebalance_only_moves_the_given_partitions(): void
    {
        $partitions = [new TopicPartition('test_topic', 1)];

        $this->consumer->expects($this->once())->method('incrementalAssign')->with($partitions);
        $this->consumer->expects($this->once())->method('incrementalUnassign')->with($partitions);
        $this->consumer->expects($this->never())->method('assign');

        $this->rebalance(RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS, $partitions, cooperative: true);
        $this->rebalance(RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS, $partitions, cooperative: true);
    }

    private function rebalance(int $err, array $partitions, bool $cooperative): void
    {
        (new ReflectionMethod($this->connection, 'rebalance'))
            ->invoke($this->connection, $this->consumer, $err, $partitions, $cooperative);
    }

    private function createConfiguredMessage(int $err = 0, array $headers = []): Message
    {
        $message = new Message();
        $message->err = $err;
        $message->headers = $headers;
        return $message;
    }

    /**
     * Simulates librdkafka calling the delivery report callback during poll() or flush().
     */
    private function deliveryReport(int $err, string $topic): void
    {
        $message = new Message();
        $message->err = $err;
        $message->topic_name = $topic;

        (new ReflectionClass($this->connection))->getProperty('deliveryReports')->getValue($this->connection)->record($message);
    }

    private function injectMockProducer(): void
    {
        $this->setPropertyValue($this->connection, 'producer', $this->producer);
    }

    private function injectMockConsumer(): void
    {
        $this->setPropertyValue($this->connection, 'consumer', $this->consumer);
    }
}
