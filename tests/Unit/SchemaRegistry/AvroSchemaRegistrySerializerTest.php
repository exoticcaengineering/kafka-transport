<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Tests\Unit\SchemaRegistry;

use Exoticca\KafkaMessenger\SchemaRegistry\Avro\AvroSchema;
use Exoticca\KafkaMessenger\SchemaRegistry\Avro\AvroSubject;
use Exoticca\KafkaMessenger\SchemaRegistry\Avro\FixedBinaryEncoding;
use Exoticca\KafkaMessenger\SchemaRegistry\Avro\FixedUnionEncoding;
use Exoticca\KafkaMessenger\SchemaRegistry\AvroSchemaRegistrySerializer;
use Exoticca\KafkaMessenger\Tests\ObjectMother\AvroSchemaMother;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;

#[CoversClass(AvroSchemaRegistrySerializer::class)]
#[CoversClass(AvroSubject::class)]
#[CoversClass(AvroSchema::class)]
#[CoversClass(FixedBinaryEncoding::class)]
#[CoversClass(FixedUnionEncoding::class)]
class AvroSchemaRegistrySerializerTest extends TestCase
{
    private AvroSchemaRegistrySerializer $serializer;

    protected function setUp(): void
    {
        $this->serializer = new AvroSchemaRegistrySerializer();
    }

    public function test_encode_decode_cluster_created(): void
    {
        $schema = AvroSchemaMother::unionType();
        $message = json_decode('
            {
                "clusterId":"01930132-f4ab-71b1-9985-f893b61bfb9c",
                "productId":16115,
                "categoryId":63376,
                "airport":"CWL",
                "calendarDateFrom":"2025-01-01",
                "calendarDateTo":"2025-01-31",
                "occurredOn":"2025-02-21T07:05:28+00:00"
            }
        ', true);

        $binary = $this->serializer->encode($message, $schema, 'cluster_created');
        $data = $this->serializer->decode($binary, $schema);

        $this->assertEquals($message, $data);
    }

    #[Test]
    public function test_encode_decode_sku_found_for_cluster(): void
    {
        $schema = AvroSchemaMother::unionType();
        $message = json_decode('
            {
                "clusterId":"01930132-f4ab-71b1-9985-f893b61bfb9c",
                "eventId":"12345",
                "skus": [
                    {
                        "productId": 16115,
                        "categoryId": 63376,
                        "categoryTypeId": 1,
                        "airportId": "CWL",
                        "departureDateAtom": "2025-01-01T00:00:00Z",
                        "landCostPrice": 500.5,
                        "flightCostPrice": 200.0,
                        "totalRetailPrice": 800.0,
                        "totalMargin": 100.5,
                        "calendarPrice": 750.0,
                        "calendarMargin": 50.0,
                        "adjustedMargin": null,
                        "categoryMinPrice": 700.0,
                        "hasGroup": false,
                        "totalQuota": 50,
                        "quotaSold": 10,
                        "quotaAvailable": 40
                    }
                ]
            }
        ', true);

        $binary = $this->serializer->encode($message, $schema, 'skus_found_for_cluster');
        $data = $this->serializer->decode($binary, $schema);

        $this->assertEquals($message, $data);
    }

    public function test_encode_decode_no_sku_found_for_cluster(): void
    {
        $schema = AvroSchemaMother::unionType();
        $message = json_decode('
            {
                "clusterId":"01930132-f4ab-71b1-9985-f893b61bfb9c"
            }
        ', true);

        $binary = $this->serializer->encode($message, $schema, 'no_skus_found_for_cluster');
        $data = $this->serializer->decode($binary, $schema);

        $this->assertEquals($message, $data);
    }

    public function test_encode_decode_with_null_margin(): void
    {
        $schema = AvroSchemaMother::unionType();
        $message = json_decode('
            {
                "clusterId":"01930132-f4ab-71b1-9985-f893b61bfb9c",
                "eventId":"12345",
                "skus": [
                    {
                        "productId": 16115,
                        "categoryId": 63376,
                        "categoryTypeId": 1,
                        "airportId": "CWL",
                        "departureDateAtom": "2025-01-01T00:00:00Z",
                        "landCostPrice": 500.5,
                        "flightCostPrice": 200.0,
                        "totalRetailPrice": 800.0,
                        "totalMargin": 100.5,
                        "calendarPrice": 750.0,
                        "calendarMargin": 50.0,
                        "adjustedMargin": null,
                        "categoryMinPrice": 700.0,
                        "hasGroup": false,
                        "totalQuota": 50,
                        "quotaSold": 10,
                        "quotaAvailable": 40
                    }
                ]
            }
        ', true);

        $binary = $this->serializer->encode($message, $schema, 'skus_found_for_cluster');
        $data = $this->serializer->decode($binary, $schema);

        $this->assertEquals($message, $data);
    }
}
