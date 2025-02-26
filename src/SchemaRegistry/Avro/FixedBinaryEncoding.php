<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\SchemaRegistry\Avro;

use Avro\AvroException;
use Avro\Model\Schema\Array_;
use Avro\Model\Schema\Enum;
use Avro\Model\Schema\Fixed;
use Avro\Model\Schema\Map;
use Avro\Model\Schema\Primitive;
use Avro\Model\Schema\Record;
use Avro\Model\Schema\Reference;
use Avro\Model\Schema\Schema;
use Avro\Model\Schema\Union;
use Avro\Serialization\Message\BinaryEncoding\ArrayEncoding;
use Avro\Serialization\Message\BinaryEncoding\ByteReader;
use Avro\Serialization\Message\BinaryEncoding\EnumEncoding;
use Avro\Serialization\Message\BinaryEncoding\FixedEncoding;
use Avro\Serialization\Message\BinaryEncoding\MapEncoding;
use Avro\Serialization\Message\BinaryEncoding\PrimitiveEncoding;
use Avro\Serialization\Message\BinaryEncoding\RecordEncoding;

use function get_class;

/**
 * This class is copied from Avro\Serialization\Message\BinaryEncoding\BinaryEncoding
 * we only modified the mapping of union types from UnionEncoding to FixedUnionEncoding to add the "tuple notation" feature that is not supported in this library.
 * + info about tuple notation => https://fastavro.readthedocs.io/en/latest/writer.html?highlight=notation#using-the-record-hint-to-specify-which-branch-of-a-union-to-take.
 */
class FixedBinaryEncoding
{
    private const ENCODING_MAP = [
        Primitive::class => PrimitiveEncoding::class,
        Fixed::class => FixedEncoding::class,
        Record::class => RecordEncoding::class,
        Enum::class => EnumEncoding::class,
        Union::class => FixedUnionEncoding::class,
        Array_::class => ArrayEncoding::class,
        Map::class => MapEncoding::class,
    ];

    /**
     * @param mixed $value
     *
     * @throws AvroException
     */
    public static function encode(Schema $schema, $value): string
    {
        if ($schema instanceof Reference) {
            $schema = $schema->getSchema();
        }

        /** @var callable $encoder */
        $encoder = [self::resolveEncoding($schema), 'encode'];

        return $encoder($schema, $value);
    }

    /**
     * @return mixed
     *
     * @throws AvroException
     */
    public static function decode(Schema $schema, ByteReader $reader)
    {
        if ($schema instanceof Reference) {
            $schema = $schema->getSchema();
        }

        /** @var callable $decoder */
        $decoder = [self::resolveEncoding($schema), 'decode'];

        return $decoder($schema, $reader);
    }

    /**
     * @throws AvroException
     */
    private static function resolveEncoding(Schema $schema): string
    {
        $realType = get_class($schema);

        if (!isset(self::ENCODING_MAP[$realType])) {
            throw AvroException::unknownType($realType);
        }

        return self::ENCODING_MAP[$realType];
    }
}
