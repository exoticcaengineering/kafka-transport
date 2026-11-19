<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Tests\Unit\SchemaRegistry\Avro;

use Avro\AvroException;
use Avro\Model\Schema\Union;
use Exoticca\KafkaMessenger\SchemaRegistry\Avro\FixedUnionEncoding;
use Exoticca\KafkaMessenger\Tests\ObjectMother\AvroSchemaMother;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(FixedUnionEncoding::class)]
final class FixedUnionEncodingTest extends TestCase
{
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
}
