<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Tests\ObjectMother;

use Apache\Avro\Schema\AvroSchema as Schema;
use Exoticca\KafkaMessenger\SchemaRegistry\Avro\AvroSchema;

class AvroSchemaMother
{
    public static function unionType(): AvroSchema
    {
        $json = json_decode(file_get_contents(__DIR__ . '/../Fixtures/schema_union.json'), true);
        return new AvroSchema(
            $json['subject'],
            Schema::parse(json_encode($json['schema'])),
            $json['id'],
            $json['version']
        );
    }

    public static function complexType(): AvroSchema
    {
        $json = json_decode(file_get_contents(__DIR__ . '/../Fixtures/schema_complex.json'), true);
        return new AvroSchema(
            $json['subject'],
            Schema::parse(json_encode($json['schema'])),
            $json['id'],
            $json['version']
        );
    }

    /**
     * Messages for schema_complex.json, with the payload the previous jaumo/avro based encoder produced.
     *
     * @return array<string, array{type: string, value: array, decoded: array, legacy_payload_hex: string}>
     */
    public static function complexTypeMessages(): array
    {
        $messages = json_decode(file_get_contents(__DIR__ . '/../Fixtures/schema_complex_messages.json'), true);

        return array_column($messages, null, 'name');
    }
}
