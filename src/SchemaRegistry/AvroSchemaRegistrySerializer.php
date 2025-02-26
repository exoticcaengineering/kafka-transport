<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\SchemaRegistry;

use Avro\Model\Schema\Union;
use Avro\Model\TypedValue;
use Avro\SchemaRegistry\Model\WireData;
use Avro\Serialization\Message\BinaryEncoding\StringByteReader;
use Exoticca\KafkaMessenger\SchemaRegistry\Avro\AvroSchema;
use Exoticca\KafkaMessenger\SchemaRegistry\Avro\FixedBinaryEncoding;
use Exoticca\KafkaMessenger\SchemaRegistry\Avro\FixedUnionEncoding;

class AvroSchemaRegistrySerializer implements SchemaRegistrySerializer
{
    public function decode(string $binary, AvroSchema $schema): array
    {
        $wiredData = WireData::fromBinary($binary);

        $typedValue = new TypedValue(
            FixedBinaryEncoding::decode($schema->getSchema(), new StringByteReader($wiredData->getMessage())),
            $schema->getSchema()
        );

        return $typedValue->getValue();
    }

    public function encode(array $record, AvroSchema $schema, ?string $typeName = null): string
    {
        if ($schema->getSchema() instanceof Union) {
            $record = [
                FixedUnionEncoding::NOTATION_TYPE_PREFIX => $typeName,
                FixedUnionEncoding::NOTATION_VALUE_PREFIX => $record,
            ];
        }

        $data = new WireData(
            $schema->getSchemaId(),
            FixedBinaryEncoding::encode($schema->getSchema(), $record)
        );

        return $data->toBinary();
    }
}
