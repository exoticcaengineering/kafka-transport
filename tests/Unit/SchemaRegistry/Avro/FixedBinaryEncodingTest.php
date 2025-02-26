<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Tests\Unit\SchemaRegistry\Avro;

use Avro\Model\Schema\Array_;
use Avro\Model\Schema\Enum;
use Avro\Model\Schema\Fixed;
use Avro\Model\Schema\Map;
use Avro\Model\Schema\Name;
use Avro\Model\Schema\NamespacedName;
use Avro\Model\Schema\Primitive;
use Avro\Model\Schema\Record;
use Avro\Serialization\Message\BinaryEncoding\ArrayEncoding;
use Avro\Serialization\Message\BinaryEncoding\EnumEncoding;
use Avro\Serialization\Message\BinaryEncoding\FixedEncoding;
use Avro\Serialization\Message\BinaryEncoding\MapEncoding;
use Avro\Serialization\Message\BinaryEncoding\PrimitiveEncoding;
use Avro\Serialization\Message\BinaryEncoding\RecordEncoding;
use Avro\Serialization\Message\BinaryEncoding\StringByteReader;
use Exoticca\KafkaMessenger\SchemaRegistry\Avro\FixedBinaryEncoding;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(FixedBinaryEncoding::class)]
class FixedBinaryEncodingTest extends TestCase
{
    public function test_encoding_primitive(): void
    {
        $schema = Primitive::fromString(Primitive::TYPE_STRING);
        $value = 'test';

        $encoded = FixedBinaryEncoding::encode($schema, $value);

        $this->assertSame(PrimitiveEncoding::encode($schema, $value), $encoded);
    }

    public function test_encode_fixed(): void
    {
        $schema = Fixed::named(NamespacedName::fromValue('test.Fixed'), 4);
        $value = 'test';

        $encoded = FixedBinaryEncoding::encode($schema, $value);

        $this->assertSame(FixedEncoding::encode($schema, $value), $encoded);
    }

    public function test_encode_record(): void
    {
        $schema = Record::named(NamespacedName::fromValue('test.Record'));
        $value = [];

        $encoded = FixedBinaryEncoding::encode($schema, $value);

        $this->assertSame(RecordEncoding::encode($schema, $value), $encoded);
    }

    public function test_encode_enum(): void
    {
        $schema = Enum::named(NamespacedName::fromValue('test.Enum'), [Name::fromValue('A'), Name::fromValue('B')]);
        $value = 'A';

        $encoded = FixedBinaryEncoding::encode($schema, $value);

        $this->assertSame(EnumEncoding::encode($schema, $value), $encoded);
    }

    public function test_encode_array(): void
    {
        $schema = Array_::of(Primitive::fromString(Primitive::TYPE_INT));
        $value = [1, 2, 3];

        $encoded = FixedBinaryEncoding::encode($schema, $value);

        $this->assertSame(ArrayEncoding::encode($schema, $value), $encoded);
    }

    public function test_encode_map(): void
    {
        $schema = Map::to(Primitive::fromString(Primitive::TYPE_STRING));
        $value = ['key1' => 'value1', 'key2' => 'value2'];

        $encoded = FixedBinaryEncoding::encode($schema, $value);

        $this->assertSame(MapEncoding::encode($schema, $value), $encoded);
    }

    public function test_decode_primitive(): void
    {
        $schema = Primitive::fromString(Primitive::TYPE_STRING);
        $reader = new StringByteReader(PrimitiveEncoding::encode($schema, 'test'));

        $decoded = FixedBinaryEncoding::decode($schema, $reader);

        $this->assertSame('test', $decoded);
    }

    public function test_decode_fixed(): void
    {
        $schema = Fixed::named(NamespacedName::fromValue('test.Fixed'), 4);
        $reader = new StringByteReader(FixedEncoding::encode($schema, 'test'));

        $decoded = FixedBinaryEncoding::decode($schema, $reader);

        $this->assertSame('test', $decoded);
    }
}
