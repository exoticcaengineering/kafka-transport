<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Tests\Unit\Transport;

use Exoticca\KafkaMessenger\SchemaRegistry\SchemaRegistryManager;
use Exoticca\KafkaMessenger\Transport\KafkaConnection;
use Exoticca\KafkaMessenger\Transport\KafkaTransportReceiver;
use Exoticca\KafkaMessenger\Transport\Stamp\KafkaMessageStamp;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use RdKafka\Message;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

#[CoversClass(KafkaMessageStamp::class)]
#[CoversClass(KafkaTransportReceiver::class)]
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

        $this->receiver = new KafkaTransportReceiver($this->connection, $this->serializer);

        $result = iterator_to_array($this->receiver->get());

        $this->assertEmpty($result);
    }

    public function test_get_with_valid_message(): void
    {
        $message = $this->createMock(Message::class);
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

        $this->receiver = new KafkaTransportReceiver($this->connection, $this->serializer);

        $result = iterator_to_array($this->receiver->get());

        $this->assertCount(1, $result);
        $this->assertInstanceOf(Envelope::class, $result[0]);
        $this->assertNotNull($result[0]->last(KafkaMessageStamp::class));
        $this->assertSame($message, $result[0]->last(KafkaMessageStamp::class)->message());
    }

    public function test_get_with_schema_registry(): void
    {
        $message = $this->createMock(Message::class);
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

        $this->receiver = new KafkaTransportReceiver($this->connection, $this->serializer, $this->schemaRegistryManager);

        $result = iterator_to_array($this->receiver->get());

        $this->assertCount(1, $result);
        $this->assertInstanceOf(Envelope::class, $result[0]);
    }

    public function test_get_with_schema_registry_and_php_serializer_throws_exception(): void
    {
        $message = $this->createMock(Message::class);
        $message->payload = '{"data":"test"}';
        $message->headers = ['header1' => 'value1'];

        $this->connection->expects($this->once())
            ->method('get')
            ->willReturn([$message]);

        $this->receiver = new KafkaTransportReceiver(
            $this->connection,
            new PhpSerializer(),
            $this->schemaRegistryManager
        );

        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('Schema registry is enabled but the defined serializer is not compatible with it.');

        iterator_to_array($this->receiver->get());
    }

    public function test_get_with_specific_queues(): void
    {
        $queueNames = ['topic1', 'topic2'];
        $message = $this->createMock(Message::class);
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

        $this->receiver = new KafkaTransportReceiver($this->connection, $this->serializer);

        $result = iterator_to_array($this->receiver->get($queueNames));

        $this->assertCount(1, $result);
    }

    public function test_ack_delegates_to_connection(): void
    {
        $message = $this->createMock(Message::class);
        $envelope = new Envelope(new \stdClass(), [
            new KafkaMessageStamp($message)
        ]);

        $this->connection->expects($this->once())
            ->method('ack')
            ->with($message);

        $this->receiver = new KafkaTransportReceiver($this->connection, $this->serializer);

        $this->receiver->ack($envelope);
    }

    public function test_reject_delegates_to_ack(): void
    {
        $message = $this->createMock(Message::class);
        $envelope = new Envelope(new \stdClass(), [
            new KafkaMessageStamp($message)
        ]);

        $this->connection->expects($this->once())
            ->method('ack')
            ->with($message);

        $this->receiver = new KafkaTransportReceiver($this->connection, $this->serializer);

        $this->receiver->reject($envelope);
    }
}
