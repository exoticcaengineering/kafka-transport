<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\SchemaRegistry;

use Exoticca\KafkaMessenger\SchemaRegistry\Avro\AvroSubject;
use RdKafka\Message;

readonly class SchemaRegistryManager
{
    public function __construct(
        private SchemaRegistrySerializer $serializer,
        private SchemaRegistryHttpClient $httpClient,
    ) {
    }

    public function decode(Message $message): array
    {
        $schema = $this->httpClient->getSubjectSchema(AvroSubject::ofValue($message->topic_name));
        return $this->serializer->decode($message->payload, $schema);
    }

    public function encode(array $body, string $topic, ?string $messageType = null): string
    {
        $schema = $this->httpClient->getSubjectSchema(AvroSubject::ofValue($topic));
        return $this->serializer->encode($body, $schema, $messageType);
    }
}
