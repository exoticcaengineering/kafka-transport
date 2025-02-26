<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\SchemaRegistry;

use Exoticca\KafkaMessenger\SchemaRegistry\Avro\AvroSchema;

interface SchemaRegistrySerializer
{
    public function decode(string $binary, AvroSchema $schema): array;

    public function encode(array $record, AvroSchema $schema, ?string $typeName = null): string;
}
