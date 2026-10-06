<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\SchemaRegistry;

use Avro\Model\Schema\Schema;
use Avro\Model\Schema\Union;
use Avro\Model\TypedValue;
use Avro\SchemaRegistry\Model\WireData;
use Avro\Serde;
use Avro\Serialization\Message\BinaryEncoding\BinaryEncoding;
use Avro\Serialization\Message\BinaryEncoding\StringByteReader;
use Exoticca\KafkaMessenger\SchemaRegistry\Avro\AvroSchema;
use Exoticca\KafkaMessenger\SchemaRegistry\Avro\AvroSubject;
use Exoticca\KafkaMessenger\SchemaRegistry\Avro\FixedBinaryEncoding;
use Exoticca\KafkaMessenger\SchemaRegistry\Avro\FixedUnionEncoding;
use RdKafka\Message;

/**
 * Schemas are cached in memory for the lifetime of the process. A schema id or a
 * subject version never changes, but a subject's latest version does, so that one expires.
 */
class SchemaRegistryManager
{
    /** @var array<int, Schema> */
    private array $schemasById = [];

    /** @var array<string, AvroSchema> */
    private array $subjectSchemas = [];

    /** @var array<string, array{AvroSchema, int}> */
    private array $latestSubjectSchemas = [];

    public function __construct(
        private readonly SchemaRegistryHttpClient $httpClient,
        private readonly int $latestSchemaTtl = 300,
    ) {
    }

    public function decode(Message $message): array
    {
        if (null === $message->payload) {
            return [];
        }

        $wiredData = WireData::fromBinary($message->payload);
        $schema = $this->schemaById($wiredData->getSchemaId());

        if ($schema instanceof Union) {
            $encoding = FixedBinaryEncoding::decode($schema, new StringByteReader($wiredData->getMessage()));
        } else {
            $encoding = BinaryEncoding::decode($schema, new StringByteReader($wiredData->getMessage()));
        }

        $typedValue = new TypedValue(
            $encoding,
            $schema
        );
        return $typedValue->getValue();

    }

    public function encode(array $body, string $topic, ?string $messageType = null, ?int $version = null): string
    {
        $schema = $this->subjectSchema(AvroSubject::ofValue($topic), $version);

        if ($schema->getSchema() instanceof Union) {
            $record = [
                FixedUnionEncoding::NOTATION_TYPE_PREFIX => $messageType,
                FixedUnionEncoding::NOTATION_VALUE_PREFIX => $body,
            ];

            $encoding = FixedBinaryEncoding::encode($schema->getSchema(), $record);
        } else {
            $encoding = BinaryEncoding::encode($schema->getSchema(), $body);
        }

        $data = new WireData(
            $schema->getSchemaId(),
            $encoding
        );
        return $data->toBinary();
    }

    private function schemaById(int $id): Schema
    {
        return $this->schemasById[$id] ??= Serde::parseSchema($this->httpClient->getSchema($id));
    }

    private function subjectSchema(AvroSubject $subject, ?int $version): AvroSchema
    {
        if ($version) {
            return $this->subjectSchemas[$subject.':'.$version] ??= $this->fetchSubjectSchema($subject, $version);
        }

        [$schema, $expiresAt] = $this->latestSubjectSchemas[(string) $subject] ?? [null, 0];
        if (null === $schema || $expiresAt <= time()) {
            $schema = $this->fetchSubjectSchema($subject);
            $this->latestSubjectSchemas[(string) $subject] = [$schema, time() + $this->latestSchemaTtl];
        }

        return $schema;
    }

    private function fetchSubjectSchema(AvroSubject $subject, ?int $version = null): AvroSchema
    {
        $schema = $this->httpClient->getSubjectSchema($subject, $version);
        $this->schemasById[$schema->getSchemaId()] ??= $schema->getSchema();

        return $schema;
    }
}
