<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\SchemaRegistry\Avro;

use Apache\Avro\AvroException;
use Apache\Avro\Datum\AvroIOBinaryEncoder;
use Apache\Avro\IO\AvroStringIO;
use Apache\Avro\Schema\AvroName;
use Apache\Avro\Schema\AvroNamedSchema;
use Apache\Avro\Schema\AvroSchema as Schema;

/**
 * Binary encoder on top of apache/avro that, unlike its AvroIODatumWriter:
 * - fills missing record fields with their schema default,
 * - supports the union "tuple notation" to pick a branch explicitly,
 *   see https://fastavro.readthedocs.io/en/latest/writer.html#using-the-tuple-notation-to-specify-which-branch-of-a-union-to-take.
 */
final class AvroDatumWriter
{
    public const NOTATION_TYPE_PREFIX = '-type';
    public const NOTATION_VALUE_PREFIX = '-value';

    public static function encode(Schema $schema, mixed $datum): string
    {
        $io = new AvroStringIO();
        self::write($schema, $datum, new AvroIOBinaryEncoder($io));

        return $io->string();
    }

    private static function write(Schema $schema, mixed $datum, AvroIOBinaryEncoder $encoder): void
    {
        if (Schema::UNION_SCHEMA === $schema->type()) {
            self::writeUnion($schema, $datum, $encoder);

            return;
        }

        if (!self::isValid($schema, $datum)) {
            throw new AvroException(sprintf('Value is not valid against the %s schema', $schema->type()));
        }

        match ($schema->type()) {
            Schema::NULL_TYPE => $encoder->writeNull($datum),
            Schema::BOOLEAN_TYPE => $encoder->writeBoolean($datum),
            Schema::INT_TYPE => $encoder->writeInt($datum),
            Schema::LONG_TYPE => $encoder->writeLong($datum),
            Schema::FLOAT_TYPE => $encoder->writeFloat($datum),
            Schema::DOUBLE_TYPE => $encoder->writeDouble($datum),
            Schema::STRING_TYPE => $encoder->writeString($datum),
            Schema::BYTES_TYPE => $encoder->writeBytes($datum),
            Schema::FIXED_SCHEMA => $encoder->write($datum),
            Schema::ENUM_SCHEMA => $encoder->writeInt($schema->symbolIndex($datum)),
            Schema::ARRAY_SCHEMA => self::writeBlocks($datum, $encoder, fn ($item) => self::write($schema->items(), $item, $encoder)),
            Schema::MAP_SCHEMA => self::writeBlocks($datum, $encoder, function ($value, $key) use ($schema, $encoder) {
                $encoder->writeString($key);
                self::write($schema->values(), $value, $encoder);
            }),
            Schema::RECORD_SCHEMA => self::writeRecord($schema, $datum, $encoder),
            default => throw new AvroException(sprintf('Unknown type: %s', $schema->type())),
        };
    }

    private static function writeRecord(Schema $schema, array $datum, AvroIOBinaryEncoder $encoder): void
    {
        foreach ($schema->fields() as $field) {
            $value = array_key_exists($field->name(), $datum) ? $datum[$field->name()] : $field->defaultValue();
            self::write($field->type(), $value, $encoder);
        }
    }

    private static function writeBlocks(array $datum, AvroIOBinaryEncoder $encoder, callable $writeItem): void
    {
        if ([] !== $datum) {
            $encoder->writeLong(count($datum));
            foreach ($datum as $key => $item) {
                $writeItem($item, $key);
            }
        }
        $encoder->writeLong(0);
    }

    private static function writeUnion(Schema $schema, mixed $datum, AvroIOBinaryEncoder $encoder): void
    {
        if (is_array($datum) && isset($datum[self::NOTATION_TYPE_PREFIX])) {
            [$index, $branch] = self::namedBranch($schema, $datum[self::NOTATION_TYPE_PREFIX]);
            $datum = $datum[self::NOTATION_VALUE_PREFIX] ?? null;
        } else {
            [$index, $branch] = self::firstValidBranch($schema, $datum);
        }

        $encoder->writeLong($index);
        self::write($branch, $datum, $encoder);
    }

    /**
     * @return array{int, Schema}
     */
    private static function namedBranch(Schema $union, string $name): array
    {
        foreach ($union->schemas() as $index => $branch) {
            if ($branch instanceof AvroNamedSchema && AvroName::extractNamespace($branch->fullname())[0] === $name) {
                return [$index, $branch];
            }
        }

        throw new AvroException(sprintf('Avro union has no branch named "%s"', $name));
    }

    /**
     * @return array{int, Schema}
     */
    private static function firstValidBranch(Schema $union, mixed $datum): array
    {
        foreach ($union->schemas() as $index => $branch) {
            if (self::isValid($branch, $datum)) {
                return [$index, $branch];
            }
        }

        throw new AvroException('Value is not valid against the union type');
    }

    /**
     * Same as AvroSchema::isValidDatum(), but a missing record field is valid when it has a default.
     */
    private static function isValid(Schema $schema, mixed $datum): bool
    {
        switch ($schema->type()) {
            case Schema::ARRAY_SCHEMA:
                if (!is_array($datum)) {
                    return false;
                }
                foreach ($datum as $item) {
                    if (!self::isValid($schema->items(), $item)) {
                        return false;
                    }
                }

                return true;
            case Schema::MAP_SCHEMA:
                if (!is_array($datum)) {
                    return false;
                }
                foreach ($datum as $key => $value) {
                    if (!is_string($key) || !self::isValid($schema->values(), $value)) {
                        return false;
                    }
                }

                return true;
            case Schema::UNION_SCHEMA:
                foreach ($schema->schemas() as $branch) {
                    if (self::isValid($branch, $datum)) {
                        return true;
                    }
                }

                return false;
            case Schema::RECORD_SCHEMA:
                if (!is_array($datum)) {
                    return false;
                }
                foreach ($schema->fields() as $field) {
                    $valid = array_key_exists($field->name(), $datum)
                        ? self::isValid($field->type(), $datum[$field->name()])
                        : $field->hasDefaultValue();
                    if (!$valid) {
                        return false;
                    }
                }

                return true;
            default:
                return Schema::isValidDatum($schema, $datum);
        }
    }
}
