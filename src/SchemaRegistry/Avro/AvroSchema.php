<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\SchemaRegistry\Avro;

use Avro\Model\Schema\Schema;

class AvroSchema
{
    public function __construct(
        private string $subject,
        private Schema $schema,
        private int $schemaId,
        private int $version
    ) {
    }

    public function getSchemaId(): int
    {
        return $this->schemaId;
    }

    public function getVersion(): int
    {
        return $this->version;
    }

    public function getSchema(): Schema
    {
        return $this->schema;
    }

    public function getSubject(): string
    {
        return $this->subject;
    }
}
