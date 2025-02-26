<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Tests\Unit\Transport;

use Exoticca\KafkaMessenger\Tests\ObjectMother\GeneralSettingMother;
use Exoticca\KafkaMessenger\Transport\KafkaConnection;
use Exoticca\KafkaMessenger\Transport\Serializer\MessageSerializer;
use Exoticca\KafkaMessenger\Transport\Setting\ConsumerSetting;
use Exoticca\KafkaMessenger\Transport\Setting\GeneralSetting;
use Exoticca\KafkaMessenger\Transport\Setting\ProducerSetting;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use RdKafka\KafkaConsumer;
use RdKafka\Message;
use RdKafka\Producer;
use RdKafka\ProducerTopic;
use ReflectionClass;
use Symfony\Component\Messenger\Exception\TransportException;

#[CoversClass(KafkaConnection::class)]
#[CoversClass(ConsumerSetting::class)]
#[CoversClass(ProducerSetting::class)]
#[CoversClass(GeneralSetting::class)]
#[CoversClass(MessageSerializer::class)]
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
        $property->setAccessible(true);
        $property->setValue($object, $value);
    }

    public function test_produce_sends_message_to_configured_topics(): void
    {
        $this->producer->expects($this->exactly(2))
            ->method('newTopic')
            ->willReturn($this->producerTopic);

        $this->producer->expects($this->exactly(2))
            ->method('poll')
            ->with(10000);

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
            ->with(10000);

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
            ->with(10000);

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

    public function test_ack_commits_message(): void
    {
        $message = new Message();

        $this->consumer->expects($this->once())
            ->method('commitAsync')
            ->with($message);

        $this->injectMockConsumer();

        $this->connection->ack($message);
    }

    public function test_ack_uses_commit_when_consume_timeout_is_zero(): void
    {
        $generalSetting = GeneralSettingMother::create([
            'consumer' => [
                'consumeTimeout' => 0
            ]
        ]);

        $this->connection = new KafkaConnection($generalSetting);

        $message = new Message();

        $this->consumer->expects($this->once())
            ->method('commit')
            ->with($message);

        $this->injectMockConsumer();

        $this->connection->ack($message);
    }

    private function createConfiguredMessage(int $err = 0, array $headers = []): Message
    {
        $message = new Message();
        $message->err = $err;
        $message->headers = $headers;
        return $message;
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
