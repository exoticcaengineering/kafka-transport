<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Tests\ObjectMother;

use Avro\Serde;
use Exoticca\KafkaMessenger\SchemaRegistry\Avro\AvroSchema;

class AvroSchemaMother
{
    public static function unionType(): AvroSchema
    {
        $json = json_decode(\Safe\file_get_contents(__DIR__ . '/../Fixtures/schema_union.json'), true);
        return new AvroSchema(
            $json['subject'],
            Serde::parseSchema(json_encode($json['schema'])),
            $json['id'],
            $json['version']
        );
    }
}
