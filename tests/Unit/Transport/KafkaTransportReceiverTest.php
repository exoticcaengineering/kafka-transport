<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Tests\Unit\Transport;

use Exoticca\KafkaMessenger\SchemaRegistry\SchemaRegistryException;
use Exoticca\KafkaMessenger\SchemaRegistry\SchemaRegistryManager;
use Exoticca\KafkaMessenger\Transport\KafkaConnection;
use Exoticca\KafkaMessenger\Transport\KafkaTransportReceiver;
use Exoticca\KafkaMessenger\Transport\Stamp\KafkaMessageStamp;
use PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use RdKafka\Message;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

#[CoversClass(KafkaMessageStamp::class)]
#[CoversClass(KafkaTransportReceiver::class)]
#[AllowMockObjectsWithoutExpectations]
final class KafkaTransportReceiverTest extends TestCase
{
    private KafkaConnection $connection;
    private SerializerInterface $serializer;
    private SchemaRegistryManager $schemaRegistryManager;
    private KafkaTransportReceiver $receiver;

    protected function setUp(): void
    {
        $this->connection = $this->createMock(KafkaConnection::class);
        $this->serializer = $this->createMock(SerializerInterface::class);
        $this->schemaRegistryManager = $this->createMock(SchemaRegistryManager::class);
    }

    public function test_get_with_empty_queue_returns_empty_array(): void
    {
        $this->connection->expects($this->once())
            ->method('get')
            ->with([])
            ->willReturn([null]);

        $this->receiver = new KafkaTransportReceiver(
            connection: $this->connection,
            serializer: $this->serializer
        );

        $result = iterator_to_array($this->receiver->get());

        $this->assertEmpty($result);
    }

    public function test_get_with_valid_message(): void
    {
        $message = new Message();
        $message->payload = '{"data":"test"}';
        $message->headers = ['header1' => 'value1'];

        $envelope = new Envelope(new \stdClass());
        $messageData = [
            'body' => '{"data":"test"}',
            'headers' => ['header1' => 'value1']
        ];

        $this->connection->expects($this->once())
            ->method('get')
            ->with([])
            ->willReturn([$message]);

        $this->serializer->expects($this->once())
            ->method('decode')
            ->with($messageData)
            ->willReturn($envelope);

        $this->receiver = new KafkaTransportReceiver(
            connection: $this->connection,
            serializer: $this->serializer
        );

        $result = iterator_to_array($this->receiver->get());

        $this->assertCount(1, $result);
        $this->assertInstanceOf(Envelope::class, $result[0]);
        $this->assertNotNull($result[0]->last(KafkaMessageStamp::class));
        $this->assertSame($message, $result[0]->last(KafkaMessageStamp::class)->message());
    }

    public function test_get_with_schema_registry(): void
    {
        $message = new Message();
        $message->payload = '{"data":"test"}';
        $message->headers = ['header1' => 'value1'];

        $envelope = new Envelope(new \stdClass());
        $decodedData = ['decoded' => 'data'];

        $this->connection->expects($this->once())
            ->method('get')
            ->willReturn([$message]);

        $this->schemaRegistryManager->expects($this->once())
            ->method('decode')
            ->with($message)
            ->willReturn($decodedData);

        $this->serializer->expects($this->once())
            ->method('decode')
            ->with($this->callback(function ($arg) use ($decodedData) {
                return isset($arg['body']) &&
                    isset($arg['headers']) &&
                    $arg['headers'] === ['header1' => 'value1'];
            }))
            ->willReturn($envelope);

        $this->receiver = new KafkaTransportReceiver(
            connection: $this->connection,
            serializer: $this->serializer,
            schemaRegistryManager: $this->schemaRegistryManager
        );

        $result = iterator_to_array($this->receiver->get());

        $this->assertCount(1, $result);
        $this->assertInstanceOf(Envelope::class, $result[0]);
    }

    public function test_get_with_schema_registry_and_php_serializer_throws_exception(): void
    {
        $message = new Message();
        $message->payload = '{"data":"test"}';
        $message->headers = ['header1' => 'value1'];

        $this->connection->expects($this->once())
            ->method('get')
            ->willReturn([$message]);

        $this->receiver = new KafkaTransportReceiver(
            connection: $this->connection,
            serializer: new PhpSerializer(),
            schemaRegistryManager:  $this->schemaRegistryManager
        );

        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('Schema registry is enabled but the defined serializer is not compatible with it.');

        iterator_to_array($this->receiver->get());
    }

    public function test_get_with_undecodable_message_sends_to_dlq_acks_and_continues(): void
    {
        $bad = new Message();
        $bad->payload = '{"data":"bad"}';
        $bad->headers = [];
        $good = new Message();
        $good->payload = '{"data":"good"}';
        $good->headers = [];

        $this->connection->method('get')->willReturn([$bad, $good]);
        $this->connection->expects($this->once())->method('produceToDlq')->with($bad);
        $this->connection->expects($this->once())->method('ack')->with($bad);
        $this->serializer->method('decode')->willReturnCallback(
            fn (array $encoded) => $encoded['body'] === $bad->payload
                ? throw new \RuntimeException('bad payload')
                : new Envelope(new \stdClass())
        );

        $this->receiver = new KafkaTransportReceiver(
            connection: $this->connection,
            serializer: $this->serializer,
        );

        $result = iterator_to_array($this->receiver->get(), false);

        $this->assertCount(1, $result);
        $this->assertSame($good, $result[0]->last(KafkaMessageStamp::class)->message());
    }

    public function test_undecodable_message_log_includes_location_and_format(): void
    {
        $message = new Message();
        $message->payload = "\0\0\0\0\x2Aavro";
        $message->headers = [];
        $message->topic_name = 'some.topic';
        $message->partition = 3;
        $message->offset = 42;

        $this->connection->method('get')->willReturn([$message]);
        $this->schemaRegistryManager->method('decode')->willThrowException(new \RuntimeException('bad avro'));

        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->once())->method('error')->with(
            $this->anything(),
            $this->callback(fn (array $context) => 'some.topic' === $context['topic']
                && 3 === $context['partition']
                && 42 === $context['offset']
                && 'avro(schema_id=42)' === $context['format']
                && 'enabled' === $context['schema_registry']
                && \RuntimeException::class === $context['error_class'])
        );

        $this->receiver = new KafkaTransportReceiver(
            connection: $this->connection,
            serializer: $this->serializer,
            schemaRegistryManager: $this->schemaRegistryManager,
            logger: $logger,
        );

        iterator_to_array($this->receiver->get());
    }

    public function test_dlq_failure_is_logged_and_message_still_acked(): void
    {
        $message = new Message();
        $message->payload = 'bad';
        $message->headers = [];

        $this->connection->method('get')->willReturn([$message]);
        $this->connection->method('produceToDlq')->willThrowException(new TransportException('DLQ topic "x" does not exist'));
        $this->connection->expects($this->once())->method('ack')->with($message);
        $this->serializer->method('decode')->willThrowException(new \RuntimeException('bad payload'));

        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->once())->method('error')->with(
            $this->anything(),
            $this->callback(fn (array $context) => 'failed (DLQ topic "x" does not exist)' === $context['dlq_topic'])
        );

        $this->receiver = new KafkaTransportReceiver(
            connection: $this->connection,
            serializer: $this->serializer,
            logger: $logger,
        );

        $this->assertEmpty(iterator_to_array($this->receiver->get()));
    }

    public function test_get_with_schema_registry_error_throws_without_ack(): void
    {
        $message = new Message();
        $message->payload = 'avro';
        $message->headers = [];

        $this->connection->method('get')->willReturn([$message]);
        $this->connection->expects($this->never())->method('produceToDlq');
        $this->connection->expects($this->never())->method('ack');
        $this->schemaRegistryManager->method('decode')->willThrowException(
            SchemaRegistryException::fromResponse(['error_code' => 50001, 'message' => 'Error in the backend data store'])
        );

        $this->receiver = new KafkaTransportReceiver(
            connection: $this->connection,
            serializer: $this->serializer,
            schemaRegistryManager: $this->schemaRegistryManager,
        );

        $this->expectException(SchemaRegistryException::class);

        iterator_to_array($this->receiver->get());
    }

    public function test_get_with_unknown_schema_id_sends_to_dlq_and_acks(): void
    {
        $message = new Message();
        $message->payload = 'avro';
        $message->headers = [];

        $this->connection->method('get')->willReturn([$message]);
        $this->connection->expects($this->once())->method('produceToDlq')->with($message);
        $this->connection->expects($this->once())->method('ack')->with($message);
        $this->schemaRegistryManager->method('decode')->willThrowException(
            SchemaRegistryException::fromResponse(['error_code' => SchemaRegistryException::SCHEMA_NOT_FOUND, 'message' => 'Schema not found'])
        );

        $this->receiver = new KafkaTransportReceiver(
            connection: $this->connection,
            serializer: $this->serializer,
            schemaRegistryManager: $this->schemaRegistryManager,
        );

        $this->assertEmpty(iterator_to_array($this->receiver->get()));
    }

    public function test_get_with_specific_queues(): void
    {
        $queueNames = ['topic1', 'topic2'];
        $message = new Message();
        $message->payload = '{"data":"test"}';
        $message->headers = ['header1' => 'value1'];

        $envelope = new Envelope(new \stdClass());

        $this->connection->expects($this->once())
            ->method('get')
            ->with($queueNames)
            ->willReturn([$message]);

        $this->serializer->expects($this->once())
            ->method('decode')
            ->willReturn($envelope);

        $this->receiver = new KafkaTransportReceiver(
            connection: $this->connection,
            serializer: $this->serializer
        );

        $result = iterator_to_array($this->receiver->get($queueNames));

        $this->assertCount(1, $result);
    }

    public function test_ack_delegates_to_connection(): void
    {
        $message = new Message();
        $envelope = new Envelope(new \stdClass(), [
            new KafkaMessageStamp($message)
        ]);

        $this->connection->expects($this->once())
            ->method('ack')
            ->with($message);

        $this->receiver = new KafkaTransportReceiver(
            connection: $this->connection,
            serializer: $this->serializer
        );

        $this->receiver->ack($envelope);
    }

    public function test_reject_delegates_to_ack(): void
    {
        $message = new Message();
        $envelope = new Envelope(new \stdClass(), [
            new KafkaMessageStamp($message)
        ]);

        $this->connection->expects($this->once())
            ->method('ack')
            ->with($message);

        $this->receiver = new KafkaTransportReceiver(
            connection: $this->connection,
            serializer: $this->serializer
        );

        $this->receiver->reject($envelope);
    }
}
