<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\SchemaRegistry\Avro;

use Avro\AvroException;
use Avro\Model\Schema\Union;
use Avro\Serialization\Message\BinaryEncoding\ByteReader;
use Avro\Serialization\Message\BinaryEncoding\PrimitiveEncoding;
use Avro\Serialization\Message\BinaryEncoding\UnionEncoding;
use Avro\Validation\Validating;

/**
 * This class is copied from Avro\Serialization\Message\BinaryEncoding\BinaryEncoding
 * For add support to tuple notation => https://fastavro.readthedocs.io/en/latest/writer.html?highlight=notation#using-the-record-hint-to-specify-which-branch-of-a-union-to-take.
 */
class FixedUnionEncoding
{
    public const NOTATION_TYPE_PREFIX = '-type';
    public const NOTATION_VALUE_PREFIX = '-value';

    /**
     * @param mixed $value
     *
     * @throws AvroException
     */
    public static function encode(Union $schema, $value): string
    {
        if (!isset($value[self::NOTATION_TYPE_PREFIX])) {
            return UnionEncoding::encode($schema, $value);
        }

        return self::encodeWithTypeNotation($schema, $value);
    }

    public static function decode(Union $schema, ByteReader $reader): array
    {
        return UnionEncoding::decode($schema, $reader);
    }

    /**
     * @param mixed $value
     *
     * @throws AvroException
     */
    private static function encodeWithTypeNotation(Union $schema, $value): string
    {
        $realValue = [];
        $index = null;
        $realSchema = null;
        $type = null;
        $keyIdentifier = $value[self::NOTATION_TYPE_PREFIX];

        foreach ($schema->getTypes() as $index => $type) {
            if ($keyIdentifier === $type->getName()) {
                $realValue = $value[self::NOTATION_VALUE_PREFIX];

                break;
            }
        }

        if (empty($realValue)) {
            throw new AvroException('Avro union without a valid identity key property');
        }

        if (Validating::isValid($realValue, $type)) {
            $realSchema = $type;
        }

        if (null === $realSchema) {
            throw new AvroException('Value is not valid against the union type');
        }

        return PrimitiveEncoding::encodeLongOrInt((int) $index).FixedBinaryEncoding::encode($realSchema, $realValue);
    }
}
