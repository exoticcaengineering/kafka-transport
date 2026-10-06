<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\SchemaRegistry\Avro;

use Apache\Avro\Datum\AvroIOBinaryDecoder;
use Apache\Avro\Datum\AvroIODatumReader;
use Apache\Avro\IO\AvroStringIO;
use Apache\Avro\Schema\AvroSchema as Schema;

/**
 * Reads data with the schema it was written with, so there's no schema resolution to do.
 */
final class AvroDatumReader extends AvroIODatumReader
{
    public static function decode(Schema $schema, string $data): mixed
    {
        return (new self($schema))->read(new AvroIOBinaryDecoder(new AvroStringIO($data)));
    }

    /**
     * The parent matches the branch against every reader branch, which warns
     * (foreach over null aliases) for records without aliases.
     */
    public function readUnion($writers_schema, $readers_schema, $decoder)
    {
        $branch = $writers_schema->schemaByIndex($decoder->readLong());

        return $this->readData($branch, $branch, $decoder);
    }
}
