<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Tests\Unit\Transport;

use Exception;
use Exoticca\KafkaMessenger\SchemaRegistry\SchemaRegistryManager;
use Exoticca\KafkaMessenger\Transport\KafkaConnection;
use Exoticca\KafkaMessenger\Transport\KafkaTransportSender;
use Exoticca\KafkaMessenger\Transport\Serializer\MessageSerializer;
use Exoticca\KafkaMessenger\Transport\Stamp\KafkaForceFlushStamp;
use Exoticca\KafkaMessenger\Transport\Stamp\KafkaMessageKeyStamp;
use Exoticca\KafkaMessenger\Transport\Stamp\KafkaMessageStamp;
use Exoticca\KafkaMessenger\Transport\Stamp\KafkaNoFlushStamp;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use RdKafka\Message;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

#[CoversClass(KafkaTransportSender::class)]
#[CoversClass(MessageSerializer::class)]
#[CoversClass(KafkaMessageStamp::class)]
#[CoversClass(KafkaMessageKeyStamp::class)]
final class KafkaTransportSenderTest extends TestCase
{
    private KafkaConnection $connection;
    private SerializerInterface $serializer;
    private SchemaRegistryManager $schemaRegistryManager;
    private KafkaTransportSender $sender;

    protected function setUp(): void
    {
        $this->connection = $this->createMock(KafkaConnection::class);
        $this->serializer = $this->createMock(SerializerInterface::class);
        $this->schemaRegistryManager = $this->createMock(SchemaRegistryManager::class);
    }

    public function test_send_with_valid_envelope(): void
    {
        $envelope = new Envelope(new \stdClass());
        $encodedEnvelope = [
            'body' => 'encoded_body',
            'headers' => [
                MessageSerializer::identifierHeaderKey() => 'message_type'
            ]
        ];

        $this->serializer->expects($this->once())
            ->method('encode')
            ->with($envelope)
            ->willReturn($encodedEnvelope);

        $this->connection->expects($this->once())
            ->method('produce')
            ->with(
                \RD_KAFKA_PARTITION_UA,
                \RD_KAFKA_CONF_OK,
                'encoded_body',
                null,
                $encodedEnvelope['headers'],
                true,
                'message_type',
                $this->isCallable()
            );

        $this->sender = new KafkaTransportSender($this->connection, null, $this->serializer);

        $result = $this->sender->send($envelope);

        $this->assertSame($envelope, $result);
    }

    public function test_send_with_schema_registry_and_php_serializer_throws_exception(): void
    {
        $envelope = new Envelope(new \stdClass());
        $encodedEnvelope = [
            'body' => 'encoded_body',
            'headers' => [
                MessageSerializer::identifierHeaderKey() => 'message_type'
            ]
        ];

        $this->serializer = new PhpSerializer();

        $this->sender = new KafkaTransportSender(
            $this->connection,
            null,
            $this->serializer,
            $this->schemaRegistryManager
        );

        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('Schema registry is enabled but the defined serializer is not compatible with it.');

        $this->sender->send($envelope);
    }

    public function test_send_with_kafka_message_key_stamp(): void
    {
        $keyStamp = new KafkaMessageKeyStamp('custom_key');
        $envelope = new Envelope(new \stdClass(), [$keyStamp]);
        $encodedEnvelope = [
            'body' => 'encoded_body',
            'headers' => [
                MessageSerializer::identifierHeaderKey() => 'message_type'
            ]
        ];

        $this->serializer->expects($this->once())
            ->method('encode')
            ->with($envelope)
            ->willReturn($encodedEnvelope);

        $this->connection->expects($this->once())
            ->method('produce')
            ->with(
                \RD_KAFKA_PARTITION_UA,
                \RD_KAFKA_CONF_OK,
                'encoded_body',
                'custom_key',
                $encodedEnvelope['headers'],
                true,
                'message_type',
                $this->isCallable()
            );

        $this->sender = new KafkaTransportSender($this->connection, null, $this->serializer);

        $result = $this->sender->send($envelope);

        $this->assertSame($envelope, $result);
    }

    public function test_send_with_both_no_flush_and_force_flush_stamps(): void
    {
        $noFlushStamp = new KafkaNoFlushStamp();
        $forceFlushStamp = new KafkaForceFlushStamp();
        $envelope = new Envelope(new \stdClass(), [$noFlushStamp, $forceFlushStamp]);
        $encodedEnvelope = [
            'body' => 'encoded_body',
            'headers' => [
                MessageSerializer::identifierHeaderKey() => 'message_type'
            ]
        ];

        $this->serializer->expects($this->once())
            ->method('encode')
            ->with($envelope)
            ->willReturn($encodedEnvelope);

        $this->connection->expects($this->once())
            ->method('produce')
            ->with(
                \RD_KAFKA_PARTITION_UA,
                \RD_KAFKA_CONF_OK,
                'encoded_body',
                null,
                $encodedEnvelope['headers'],
                true,
                'message_type',
                $this->isCallable()
            );

        $this->sender = new KafkaTransportSender($this->connection, null, $this->serializer);

        $result = $this->sender->send($envelope);

        $this->assertSame($envelope, $result);
    }

    public function test_send_with_missing_identifier_throws_exception(): void
    {
        $envelope = new Envelope(new \stdClass());
        $encodedEnvelope = [
            'body' => 'encoded_body',
            'headers' => []  // Missing identifier
        ];

        $this->serializer->expects($this->once())
            ->method('encode')
            ->with($envelope)
            ->willReturn($encodedEnvelope);

        $this->sender = new KafkaTransportSender($this->connection, null, $this->serializer);

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Discriminatory name not found in envelope');

        $this->sender->send($envelope);
    }

    public function test_send_with_connection_exception(): void
    {
        $envelope = new Envelope(new \stdClass());
        $encodedEnvelope = [
            'body' => 'encoded_body',
            'headers' => [
                MessageSerializer::identifierHeaderKey() => 'message_type'
            ]
        ];

        $this->serializer->expects($this->once())
            ->method('encode')
            ->with($envelope)
            ->willReturn($encodedEnvelope);

        $this->connection->expects($this->once())
            ->method('produce')
            ->willThrowException(new Exception('Connection error'));

        $this->sender = new KafkaTransportSender($this->connection, null, $this->serializer);

        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('Connection error');

        $this->sender->send($envelope);
    }

    public function test_encode_with_schema_registry(): void
    {
        $body = '{"data":"test"}';
        $topic = 'test-topic';
        $typeName = 'message_type';
        $encodedBody = 'encoded_body';

        $this->schemaRegistryManager->expects($this->once())
            ->method('encode')
            ->with(['data' => 'test'], $topic, $typeName)
            ->willReturn($encodedBody);

        $this->sender = new KafkaTransportSender($this->connection, null, $this->serializer, $this->schemaRegistryManager);

        $result = $this->sender->encodeWithSchemaRegistry($topic, $body, $typeName);

        $this->assertEquals($encodedBody, $result);
    }

    public function test_encode_with_schema_registry_returns_original_body_when_registry_is_null(): void
    {
        $body = '{"data":"test"}';
        $topic = 'test-topic';
        $typeName = 'message_type';

        $this->sender = new KafkaTransportSender($this->connection, null, $this->serializer);

        $result = $this->sender->encodeWithSchemaRegistry($topic, $body, $typeName);

        $this->assertEquals($body, $result);
    }
}
