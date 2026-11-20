<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Tests\Unit\SchemaRegistry\Avro;

use Avro\AvroException;
use Avro\Model\Schema\Union;
use Avro\Serde;
use Exoticca\KafkaMessenger\SchemaRegistry\Avro\FixedUnionEncoding;
use Exoticca\KafkaMessenger\Tests\ObjectMother\AvroSchemaMother;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(FixedUnionEncoding::class)]
final class FixedUnionEncodingTest extends TestCase
{
    public function test_encodes_the_branch_with_the_record_name(): void
    {
        // Branch index 1 and int 1 are both the zigzag varint 0x02.
        $this->assertSame("\x02\x02", FixedUnionEncoding::encode(self::union(), [
            FixedUnionEncoding::NOTATION_TYPE_PREFIX => 'booking_created',
            FixedUnionEncoding::NOTATION_VALUE_PREFIX => ['id' => 1],
        ]));
    }

    public function test_encodes_an_empty_payload_for_a_record_without_fields(): void
    {
        $this->assertSame("\x00", FixedUnionEncoding::encode(self::union(), [
            FixedUnionEncoding::NOTATION_TYPE_PREFIX => 'heartbeat',
            FixedUnionEncoding::NOTATION_VALUE_PREFIX => [],
        ]));
    }

    public function test_unknown_record_name_names_the_union_records(): void
    {
        $schema = AvroSchemaMother::unionType()->getSchema();
        \assert($schema instanceof Union);

        $this->expectException(AvroException::class);
        $this->expectExceptionMessage('Avro union has no record named "cluster-created". Union records: "cluster_created", "skus_found_for_cluster", "no_skus_found_for_cluster"');

        FixedUnionEncoding::encode($schema, [
            FixedUnionEncoding::NOTATION_TYPE_PREFIX => 'cluster-created',
            FixedUnionEncoding::NOTATION_VALUE_PREFIX => ['clusterId' => 'c1'],
        ]);
    }

    private static function union(): Union
    {
        $schema = Serde::parseSchema(json_encode([
            ['type' => 'record', 'name' => 'heartbeat', 'namespace' => 'com.exoticca', 'fields' => []],
            ['type' => 'record', 'name' => 'booking_created', 'namespace' => 'com.exoticca', 'fields' => [['name' => 'id', 'type' => 'int']]],
        ]));
        \assert($schema instanceof Union);

        return $schema;
    }
}
