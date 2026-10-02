<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Transport;

use Avro\SchemaRegistry\ClientError;
use Avro\SchemaRegistry\Model\Error;
use Exoticca\KafkaMessenger\SchemaRegistry\SchemaRegistryManager;
use Exoticca\KafkaMessenger\Transport\Metadata\KafkaMetadataHookInterface;
use Exoticca\KafkaMessenger\Transport\Stamp\KafkaMessageStamp;
use Psr\Log\LoggerInterface;
use RdKafka\Message;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Transport\Receiver\ReceiverInterface;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Contracts\HttpClient\Exception\ExceptionInterface as HttpClientExceptionInterface;

final class KafkaTransportReceiver implements ReceiverInterface
{
    public function __construct(
        private KafkaConnection      $connection,
        private ?KafkaMetadataHookInterface $metadata = null,
        private ?SerializerInterface $serializer = new PhpSerializer(),
        private ?SchemaRegistryManager $schemaRegistryManager = null,
        private ?LoggerInterface $logger = null,
    ) {
    }

    public function get(array $queues = []): iterable
    {
        /** @var ?Message $message */
        foreach ($this->connection->get($queues) as $message) {
            if (!$message) {
                return [];
            }
            yield from $this->getEnvelope($message);
        }
    }

    public function ack(Envelope $envelope): void
    {
        $this->connection->ack($envelope->last(KafkaMessageStamp::class)->message());
    }

    public function reject(Envelope $envelope): void
    {
        $this->ack($envelope);
    }

    private function getEnvelope(Message $message): iterable
    {
        if ($this->schemaRegistryManager && $this->serializer instanceof PhpSerializer) {
            throw new TransportException('Schema registry is enabled but the defined serializer is not compatible with it. You must use a different serializer.');
        }

        $rawPayload = $message->payload;

        try {
            if ($this->schemaRegistryManager) {
                $message->payload = json_encode($this->schemaRegistryManager->decode($message));
            }

            $messageToConvertToEnvelope = [
                'body' => $message->payload,
                'headers' => $message->headers,
            ];

            $envelope = $this->serializer->decode($messageToConvertToEnvelope);
        } catch (ClientError|HttpClientExceptionInterface $e) {
            // Schema Registry unavailable: not the message's fault, keep failing so nothing is acked away.
            // An unknown schema id is the message's fault, though, and would fail forever.
            if (Error::SCHEMA_NOT_FOUND !== $e->getCode()) {
                throw $e;
            }
            $this->handleUndecodable($message, $rawPayload, $e);

            return;
        } catch (\Throwable $e) {
            $this->handleUndecodable($message, $rawPayload, $e);

            return;
        }

        if ($this->metadata) {
            $envelope = $this->metadata->afterConsume($envelope);
        }

        yield $envelope->with(new KafkaMessageStamp($message));
    }

    /**
     * Messenger's worker doesn't catch receiver errors, so a message that can't be
     * decoded would stop the consumer and, never being acked, be read again on restart.
     */
    private function handleUndecodable(Message $message, ?string $rawPayload, \Throwable $e): void
    {
        $message->payload = $rawPayload;

        // A DLQ failure must not block the partition either. The message is still in the source topic
        // at the logged offset until retention, so it's acked anyway.
        try {
            $dlqTopic = $this->connection->produceToDlq($message, $e) ?? 'none';
        } catch (\Throwable $dlqError) {
            $dlqTopic = 'failed ('.$dlqError->getMessage().')';
        }

        // Everything goes in the message too: Symfony's default logger drops context it doesn't interpolate.
        $this->logger?->error('Kafka message could not be decoded: {error} ({error_class}) topic={topic} partition={partition} offset={offset} format={format} schema_registry={schema_registry} dlq_topic={dlq_topic}', [
            'error' => $e->getMessage(),
            'error_class' => $e::class,
            'topic' => $message->topic_name,
            'partition' => $message->partition,
            'offset' => $message->offset,
            'format' => self::detectFormat($rawPayload),
            'schema_registry' => $this->schemaRegistryManager ? 'enabled' : 'disabled',
            'dlq_topic' => $dlqTopic,
            'exception' => $e,
        ]);
        $this->connection->ack($message);
    }

    /**
     * Confluent wire format is a 0x00 magic byte followed by a 4-byte big-endian schema id.
     */
    private static function detectFormat(?string $payload): string
    {
        if (null === $payload || '' === $payload) {
            return 'empty';
        }
        if ("\0" === $payload[0] && \strlen($payload) >= 5) {
            return 'avro(schema_id='.unpack('N', $payload, 1)[1].')';
        }

        return 'not-avro';
    }
}
