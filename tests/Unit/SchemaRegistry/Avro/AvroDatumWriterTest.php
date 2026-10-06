<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Tests\Unit\SchemaRegistry\Avro;

use Apache\Avro\AvroException;
use Apache\Avro\Schema\AvroSchema as Schema;
use Exoticca\KafkaMessenger\SchemaRegistry\Avro\AvroDatumReader;
use Exoticca\KafkaMessenger\SchemaRegistry\Avro\AvroDatumWriter;
use Exoticca\KafkaMessenger\Tests\ObjectMother\AvroSchemaMother;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

#[CoversClass(AvroDatumWriter::class)]
#[CoversClass(AvroDatumReader::class)]
class AvroDatumWriterTest extends TestCase
{
    private const CLUSTER_CREATED = [
        'clusterId' => 'c1',
        'productId' => 1,
        'categoryId' => 2,
        'airport' => 'BCN',
        'calendarDateFrom' => '2026-01-01',
        'calendarDateTo' => '2026-02-01',
        'occurredOn' => '2026-01-01',
    ];

    public function test_union_tuple_notation_selects_named_branch(): void
    {
        $schema = AvroSchemaMother::unionType()->getSchema();

        $encoded = AvroDatumWriter::encode($schema, [
            AvroDatumWriter::NOTATION_TYPE_PREFIX => 'cluster_created',
            AvroDatumWriter::NOTATION_VALUE_PREFIX => self::CLUSTER_CREATED,
        ]);

        // Same bytes the previous jaumo/avro based encoder produced.
        $this->assertSame('0004633102040642434e14323032362d30312d303114323032362d30322d303114323032362d30312d3031', bin2hex($encoded));
        $this->assertSame(self::CLUSTER_CREATED, AvroDatumReader::decode($schema, $encoded));
    }

    public function test_union_tuple_notation_with_unknown_branch_throws(): void
    {
        $this->expectException(AvroException::class);

        AvroDatumWriter::encode(AvroSchemaMother::unionType()->getSchema(), [
            AvroDatumWriter::NOTATION_TYPE_PREFIX => 'unknown',
            AvroDatumWriter::NOTATION_VALUE_PREFIX => self::CLUSTER_CREATED,
        ]);
    }

    public function test_union_tuple_notation_with_invalid_value_throws(): void
    {
        $this->expectException(AvroException::class);

        AvroDatumWriter::encode(AvroSchemaMother::unionType()->getSchema(), [
            AvroDatumWriter::NOTATION_TYPE_PREFIX => 'no_skus_found_for_cluster',
            AvroDatumWriter::NOTATION_VALUE_PREFIX => ['clusterId' => 1],
        ]);
    }

    public function test_missing_fields_get_their_default(): void
    {
        $schema = AvroSchemaMother::unionType()->getSchema();

        $encoded = AvroDatumWriter::encode($schema, [
            AvroDatumWriter::NOTATION_TYPE_PREFIX => 'skus_found_for_cluster',
            AvroDatumWriter::NOTATION_VALUE_PREFIX => ['clusterId' => 'c1', 'skus' => []],
        ]);

        $this->assertSame(
            ['clusterId' => 'c1', 'eventId' => 'undefined', 'skus' => []],
            AvroDatumReader::decode($schema, $encoded)
        );
    }

    public function test_missing_field_without_default_throws(): void
    {
        $this->expectException(AvroException::class);

        AvroDatumWriter::encode(self::recordSchema(), ['id' => 'a']);
    }

    public function test_round_trip_of_every_type(): void
    {
        $schema = self::recordSchema();
        $value = [
            'id' => 'a',
            'count' => -5,
            'big' => PHP_INT_MAX,
            'ratio' => 1.5,
            'price' => 99.9,
            'active' => false,
            'color' => 'GREEN',
            'tags' => ['x', 'y'],
            'attrs' => ['k' => 1, 'j' => -2],
            'note' => 'hi',
            'amount' => 3,
            'hash' => 'abcd',
        ];

        $this->assertSame($value, AvroDatumReader::decode($schema, AvroDatumWriter::encode($schema, $value)));
    }

    public function test_plain_union_takes_first_valid_branch(): void
    {
        $schema = self::recordSchema();
        $value = ['id' => 'a', 'count' => 0, 'big' => 0, 'ratio' => 0.0, 'price' => 1, 'active' => true, 'color' => 'RED', 'tags' => [], 'attrs' => [], 'hash' => 'abcd'];

        $decoded = AvroDatumReader::decode($schema, AvroDatumWriter::encode($schema, $value + ['amount' => 2.5]));
        $this->assertSame(2.5, $decoded['amount']);
        $this->assertNull($decoded['note']);

        $decoded = AvroDatumReader::decode($schema, AvroDatumWriter::encode($schema, $value + ['amount' => 2]));
        $this->assertSame(2, $decoded['amount']);
    }

    public static function complexMessages(): iterable
    {
        foreach (AvroSchemaMother::complexTypeMessages() as $name => $message) {
            yield $name => [$message['type'], $message['value'], $message['decoded'], $message['legacy_payload_hex']];
        }
    }

    #[DataProvider('complexMessages')]
    public function test_decodes_payloads_written_by_the_previous_encoder(string $type, array $value, array $decoded, string $legacyPayloadHex): void
    {
        $schema = AvroSchemaMother::complexType()->getSchema();

        $this->assertSame($decoded, AvroDatumReader::decode($schema, hex2bin($legacyPayloadHex)));
    }

    #[DataProvider('complexMessages')]
    public function test_round_trip_of_complex_messages(string $type, array $value, array $decoded): void
    {
        $schema = AvroSchemaMother::complexType()->getSchema();

        $encoded = AvroDatumWriter::encode($schema, [
            AvroDatumWriter::NOTATION_TYPE_PREFIX => $type,
            AvroDatumWriter::NOTATION_VALUE_PREFIX => $value,
        ]);

        $this->assertSame($decoded, AvroDatumReader::decode($schema, $encoded));
    }

    public function test_named_type_referenced_from_another_union_branch(): void
    {
        $schema = Schema::parse(json_encode([
            ['type' => 'record', 'name' => 'created', 'namespace' => 'com.exoticca', 'fields' => [
                ['name' => 'customer', 'type' => ['type' => 'record', 'name' => 'Customer', 'fields' => [['name' => 'id', 'type' => 'long']]]],
            ]],
            ['type' => 'record', 'name' => 'cancelled', 'namespace' => 'com.exoticca.cancellation', 'fields' => [
                ['name' => 'customer', 'type' => 'com.exoticca.Customer'],
            ]],
        ]));

        $encoded = AvroDatumWriter::encode($schema, [
            AvroDatumWriter::NOTATION_TYPE_PREFIX => 'cancelled',
            AvroDatumWriter::NOTATION_VALUE_PREFIX => ['customer' => ['id' => 7]],
        ]);

        $this->assertSame("", $encoded);
        $this->assertSame(['customer' => ['id' => 7]], AvroDatumReader::decode($schema, $encoded));
    }

    private static function recordSchema(): Schema
    {
        return Schema::parse(json_encode([
            'type' => 'record',
            'name' => 'everything',
            'fields' => [
                ['name' => 'id', 'type' => 'string'],
                ['name' => 'count', 'type' => 'int'],
                ['name' => 'big', 'type' => 'long'],
                ['name' => 'ratio', 'type' => 'float'],
                ['name' => 'price', 'type' => 'double'],
                ['name' => 'active', 'type' => 'boolean'],
                ['name' => 'color', 'type' => ['type' => 'enum', 'name' => 'Color', 'symbols' => ['RED', 'GREEN']]],
                ['name' => 'tags', 'type' => ['type' => 'array', 'items' => 'string']],
                ['name' => 'attrs', 'type' => ['type' => 'map', 'values' => 'int']],
                ['name' => 'note', 'type' => ['null', 'string'], 'default' => null],
                ['name' => 'amount', 'type' => ['null', 'int', 'double'], 'default' => null],
                ['name' => 'hash', 'type' => ['type' => 'fixed', 'name' => 'Hash', 'size' => 4]],
            ],
        ]));
    }
}
