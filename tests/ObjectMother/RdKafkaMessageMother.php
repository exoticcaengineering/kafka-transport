<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Tests\ObjectMother;

use RdKafka\Message;

class RdKafkaMessageMother
{
    public static function create(
        int $err = 0,
        string $topic_name = 'default-topic',
        int $partition = 0,
        ?string $payload = null,
        ?int $len = null,
        ?string $key = null,
        int $offset = 0,
        int $timestamp = 0,
        ?array $headers = null,
        ?string $opaque = null
    ): Message {
        $message = new Message();
        $message->err = $err;
        $message->topic_name = $topic_name;
        $message->partition = $partition;
        $message->payload = $payload;
        $message->len = $len;
        $message->key = $key;
        $message->offset = $offset;
        $message->timestamp = $timestamp;
        $message->headers = $headers;
        $message->opaque = $opaque;

        return $message;
    }

    public static function valid(): Message
    {
        return self::create(
            0,
            'test-topic',
            1,
            'Sample payload',
            100,
            'key123',
            10,
            time(),
            ['header1' => 'value1'],
            'opaqueData'
        );
    }

    public static function invalid(): Message
    {
        return self::create(
            1,
            'invalid-topic',
            0,
            null,
            null,
            null,
            -1,
            0,
            null,
            null
        );
    }
}
