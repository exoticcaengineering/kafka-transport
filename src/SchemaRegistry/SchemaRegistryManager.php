<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\SchemaRegistry;

use Apache\Avro\Schema\AvroSchema as Schema;
use Exoticca\KafkaMessenger\SchemaRegistry\Avro\AvroDatumReader;
use Exoticca\KafkaMessenger\SchemaRegistry\Avro\AvroDatumWriter;
use Exoticca\KafkaMessenger\SchemaRegistry\Avro\AvroSchema;
use Exoticca\KafkaMessenger\SchemaRegistry\Avro\AvroSubject;
use RdKafka\Message;

/**
 * Schemas are cached in memory for the lifetime of the process. A schema id or a
 * subject version never changes, but a subject's latest version does, so that one expires.
 */
class SchemaRegistryManager
{
    private const MAGIC_BYTE = "\0";
    private const HEADER_SIZE = 5;

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

        if (strlen($message->payload) < self::HEADER_SIZE || self::MAGIC_BYTE !== $message->payload[0]) {
            throw new \InvalidArgumentException('Payload is not in the Schema Registry wire format');
        }

        $schemaId = unpack('N', $message->payload, 1)[1];

        return AvroDatumReader::decode($this->schemaById($schemaId), substr($message->payload, self::HEADER_SIZE));
    }

    public function encode(array $body, string $topic, ?string $messageType = null, ?int $version = null): string
    {
        $schema = $this->subjectSchema(AvroSubject::ofValue($topic), $version);

        if (Schema::UNION_SCHEMA === $schema->getSchema()->type()) {
            $body = [
                AvroDatumWriter::NOTATION_TYPE_PREFIX => $messageType,
                AvroDatumWriter::NOTATION_VALUE_PREFIX => $body,
            ];
        }

        return self::MAGIC_BYTE.pack('N', $schema->getSchemaId()).AvroDatumWriter::encode($schema->getSchema(), $body);
    }

    private function schemaById(int $id): Schema
    {
        return $this->schemasById[$id] ??= Schema::parse($this->httpClient->getSchema($id));
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
