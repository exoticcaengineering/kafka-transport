<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\SchemaRegistry;

/**
 * Error returned by the Schema Registry API, the code is its "error_code".
 */
class SchemaRegistryException extends \RuntimeException
{
    public const SUBJECT_NOT_FOUND = 40401;
    public const SCHEMA_NOT_FOUND = 40403;

    public static function fromResponse(array $response): self
    {
        return new self($response['message'] ?? 'Unknown Schema Registry error', (int) $response['error_code']);
    }

    public static function invalidJson(string $raw, \JsonException $previous): self
    {
        return new self(sprintf('Schema Registry returned invalid JSON: %s', $raw), 0, $previous);
    }
}
