<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Tests\Unit\SchemaRegistry;

use Exoticca\KafkaMessenger\SchemaRegistry\SchemaRegistryHttpClient;
use Exoticca\KafkaMessenger\SchemaRegistry\SchemaRegistryManager;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use RdKafka\Message;
use Symfony\Component\HttpClient\MockHttpClient;
use Symfony\Component\HttpClient\Response\MockResponse;

#[CoversClass(SchemaRegistryManager::class)]
class SchemaRegistryManagerTest extends TestCase
{
    private const SCHEMA = '{"type":"record","name":"test","fields":[{"name":"name","type":"string"}]}';

    /** @var string[] */
    private array $requestedUrls = [];

    public function test_encode_and_decode_use_the_wire_format(): void
    {
        $manager = $this->manager();

        $encoded = $manager->encode(['name' => 'foo'], 'topic');

        $this->assertSame("\0".pack('N', 42)."\x06foo", $encoded);
        $this->assertSame(['name' => 'foo'], $manager->decode($this->message($encoded)));
    }

    public function test_decode_fetches_each_schema_id_once(): void
    {
        $manager = $this->manager();

        $manager->decode($this->message("\0".pack('N', 42)."\x06foo"));
        $manager->decode($this->message("\0".pack('N', 42)."\x06bar"));

        $this->assertSame(['http://registry.local/schemas/ids/42'], $this->requestedUrls);
    }

    public function test_latest_subject_schema_is_cached_until_ttl(): void
    {
        $manager = $this->manager();

        $manager->encode(['name' => 'foo'], 'topic');
        $manager->encode(['name' => 'bar'], 'topic');

        $this->assertSame(['http://registry.local/subjects/topic-value/versions/latest'], $this->requestedUrls);
    }

    public function test_latest_subject_schema_is_fetched_again_after_ttl(): void
    {
        $manager = $this->manager(latestSchemaTtl: 0);

        $manager->encode(['name' => 'foo'], 'topic');
        $manager->encode(['name' => 'bar'], 'topic');

        $this->assertCount(2, $this->requestedUrls);
    }

    public function test_versioned_subject_schema_is_cached(): void
    {
        $manager = $this->manager(latestSchemaTtl: 0);

        $manager->encode(['name' => 'foo'], 'topic', version: 3);
        $manager->encode(['name' => 'bar'], 'topic', version: 3);

        $this->assertSame(['http://registry.local/subjects/topic-value/versions/3'], $this->requestedUrls);
    }

    public function test_encode_primes_the_schema_id_cache(): void
    {
        $manager = $this->manager();

        $manager->decode($this->message($manager->encode(['name' => 'foo'], 'topic')));

        $this->assertSame(['http://registry.local/subjects/topic-value/versions/latest'], $this->requestedUrls);
    }

    public function test_decode_without_wire_format_throws(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->manager()->decode($this->message('{"name":"foo"}'));
    }

    public function test_decode_null_payload(): void
    {
        $this->assertSame([], $this->manager()->decode($this->message(null)));
        $this->assertSame([], $this->requestedUrls);
    }

    private function manager(int $latestSchemaTtl = 300): SchemaRegistryManager
    {
        $httpClient = new MockHttpClient(function (string $method, string $url) {
            $this->requestedUrls[] = $url;

            return new MockResponse(json_encode(
                str_contains($url, '/schemas/ids/')
                    ? ['schema' => self::SCHEMA]
                    : ['subject' => 'topic-value', 'version' => 3, 'id' => 42, 'schema' => self::SCHEMA]
            ));
        });

        return new SchemaRegistryManager(
            new SchemaRegistryHttpClient('http://registry.local', 'key', 'secret', $httpClient),
            $latestSchemaTtl,
        );
    }

    private function message(?string $payload): Message
    {
        $message = new Message();
        $message->payload = $payload;

        return $message;
    }
}
