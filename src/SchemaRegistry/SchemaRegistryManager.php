<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\SchemaRegistry;

use Avro\Model\Schema\Union;
use Avro\Model\TypedValue;
use Avro\SchemaRegistry\Model\WireData;
use Avro\Serde;
use Avro\Serialization\Message\BinaryEncoding\BinaryEncoding;
use Avro\Serialization\Message\BinaryEncoding\StringByteReader;
use Exoticca\KafkaMessenger\SchemaRegistry\Avro\AvroSubject;
use Exoticca\KafkaMessenger\SchemaRegistry\Avro\FixedBinaryEncoding;
use Exoticca\KafkaMessenger\SchemaRegistry\Avro\FixedUnionEncoding;
use RdKafka\Message;

readonly class SchemaRegistryManager
{
    public function __construct(
        private SchemaRegistryHttpClient $httpClient,
    ) {
    }

    public function decode(Message $message): array
    {
        if (null === $message->payload) {
            return [];
        }

        $wiredData = WireData::fromBinary($message->payload);
        $schema = Serde::parseSchema($this->httpClient->getSchema($wiredData->getSchemaId()));

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

    public function encode(array $body, string $topic, ?string $messageType = null, int $version = null): string
    {
        $schema = $this->httpClient->getSubjectSchema(AvroSubject::ofValue($topic), $version);

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
}
