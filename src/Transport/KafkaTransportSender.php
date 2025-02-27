<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Transport;

use Exception;
use Exoticca\KafkaMessenger\SchemaRegistry\SchemaRegistryManager;
use Exoticca\KafkaMessenger\Transport\Metadata\KafkaMetadataHookInterface;
use Exoticca\KafkaMessenger\Transport\Serializer\MessageSerializer;
use Exoticca\KafkaMessenger\Transport\Stamp\KafkaForceFlushStamp;
use Exoticca\KafkaMessenger\Transport\Stamp\KafkaMessageKeyStamp;
use Exoticca\KafkaMessenger\Transport\Stamp\KafkaMessageVersionStamp;
use Exoticca\KafkaMessenger\Transport\Stamp\KafkaNoFlushStamp;
use Exoticca\KafkaMessenger\Transport\Stamp\KafkaMessageStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

final class KafkaTransportSender implements SenderInterface
{
    public function __construct(
        private KafkaConnection             $connection,
        private ?KafkaMetadataHookInterface $metadata = null,
        private ?SerializerInterface        $serializer = new PhpSerializer(),
        private ?SchemaRegistryManager      $schemaRegistryManager = null,
    ) {
    }

    public function send(Envelope $envelope): Envelope
    {
        $targetVersion = $envelope->last(KafkaMessageVersionStamp::class);

        if ($this->metadata) {
            $envelope = $this->metadata->addMetadata($envelope);
        }

        $decodedEnvelope = $this->serializer->encode($envelope);

        if ($this->schemaRegistryManager && $this->serializer instanceof PhpSerializer) {
            throw new TransportException('Schema registry is enabled but the defined serializer is not compatible with it. You must use a different serializer.');
        }

        $key = null;
        $partition = null;
        $messageFlags = null;

        if ($messageStamp = $envelope->last(KafkaMessageStamp::class)) {
            $partition = $messageStamp->partition ?? null;
            $messageFlags = $messageStamp->messageFlags ?? null;
            $key = $messageStamp->key ?? null;
        }

        if ($keyStamp = $envelope->last(KafkaMessageKeyStamp::class)) {
            $key = $keyStamp->key;
        }

        $forceFlush = true;

        if ($envelope->last(KafkaNoFlushStamp::class)) {
            $forceFlush = false;
        }

        if ($envelope->last(KafkaNoFlushStamp::class) && $envelope->last(KafkaForceFlushStamp::class)) {
            $forceFlush = true;
        }

        $identifier = $decodedEnvelope["headers"][MessageSerializer::identifierHeaderKey()] ?? throw new Exception('Discriminatory name not found in envelope');

        if (!$partition) {
            $partition = \RD_KAFKA_PARTITION_UA;
        }

        if (!$messageFlags) {
            $messageFlags = \RD_KAFKA_CONF_OK;
        }

        try {
            $this->connection->produce(
                partition: $partition,
                messageFlags: $messageFlags,
                body: $decodedEnvelope["body"],
                key: $key,
                headers: $decodedEnvelope["headers"] ?? [],
                forceFlush: $forceFlush,
                identifier: $identifier,
                beforeProduceConvertBody: fn (string $topic) => $this->encodeWithSchemaRegistry($topic, $decodedEnvelope["body"], $identifier, $targetVersion)
            );
        } catch (Exception $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }

        return $envelope;
    }

    public function encodeWithSchemaRegistry(
        string $topic,
        string $body,
        ?string $typeName = null,
        ?int $targetVersion = null
    ): string {
        if ($this->schemaRegistryManager) {
            $body = $this->schemaRegistryManager->encode(
                body: json_decode($body, true),
                topic: $topic,
                messageType: $typeName,
                version: $targetVersion
            );
        }
        return $body;
    }

}
