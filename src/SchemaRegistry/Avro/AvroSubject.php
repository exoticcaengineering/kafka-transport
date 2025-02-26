<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\SchemaRegistry\Avro;

class AvroSubject
{
    private const SCHEMA_VALUE_REGISTRY_SUFFIX = '-value';
    private const SCHEMA_KEY_REGISTRY_SUFFIX = '-key';

    private function __construct(private string $subject)
    {
    }

    public function __toString(): string
    {
        return $this->subject;
    }

    public static function ofKey(string $name): self
    {
        return new self($name.self::SCHEMA_KEY_REGISTRY_SUFFIX);
    }

    public static function ofValue(string $name): self
    {
        return new self($name.self::SCHEMA_VALUE_REGISTRY_SUFFIX);
    }
}
