<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Tests\Unit\Transport;

use Exoticca\KafkaMessenger\Tests\Fixtures\TestMessage;
use Exoticca\KafkaMessenger\Transport\KafkaTransportSettingResolver;
use Exoticca\KafkaMessenger\Transport\Setting\ConsumerSetting;
use Exoticca\KafkaMessenger\Transport\Setting\GeneralSetting;
use Exoticca\KafkaMessenger\Transport\Setting\KafkaOptionList;
use Exoticca\KafkaMessenger\Transport\Setting\ProducerSetting;
use Exoticca\KafkaMessenger\Transport\Setting\SettingManager;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(KafkaTransportSettingResolver::class)]
#[CoversClass(KafkaOptionList::class)]
#[CoversClass(SettingManager::class)]
#[CoversClass(ProducerSetting::class)]
#[CoversClass(ConsumerSetting::class)]
#[CoversClass(GeneralSetting::class)]
final class KafkaTransportSettingResolverTest extends TestCase
{
    private KafkaTransportSettingResolver $resolver;

    protected function setUp(): void
    {
        $this->resolver = new KafkaTransportSettingResolver();
    }

    public function test_resolve_with_valid_options(): void
    {
        $dsn = 'kafka://broker:9092';
        $transportOptions = [
            'transport_name' => 'kafka_test',
            'identifier' => ['staticMethod' => 'getType'],
            'topics' => ['test-topic'],
            'consumer' => [
                'routing' => [
                    ['name' => 'message', 'class' => TestMessage::class],
                ],
                'config' => ['group.id' => 'test-group'],
            ],
            'producer' => [
                'routing' => [
                    ['name' => 'message', 'topic' => 'test-topic'],
                ],
            ],
        ];

        $result = $this->resolver->resolve($dsn, [], $transportOptions);

        $this->assertInstanceOf(GeneralSetting::class, $result);
        $this->assertEquals('broker:9092', $result->host);
        $this->assertEquals('kafka_test', $result->transportName);
        $this->assertEquals('getType', $result->staticMethodIdentifier);
        $this->assertEquals(['message' => TestMessage::class], $result->consumer->routing);
        $this->assertEquals(['message' => 'test-topic'], $result->producer->routing);
        $this->assertEquals(['test-topic'], $result->consumer->topics);
        $this->assertEquals(['test-topic'], $result->producer->topics);
    }

    public function test_invalid_dsn_throws_exception(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('The given Kafka DSN "invalid_dsn" is invalid.');
        $this->resolver->resolve('invalid_dsn', [], ['transport_name' => 'test']);
    }

    public function test_missing_topics_throws_exception(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('At least one of "consumer.topics", "producer.topics" or "topics" options is required');

        $this->resolver->resolve('kafka://broker:9092', [], [
            'transport_name' => 'kafka_test',
            'identifier' => ['staticMethod' => 'getType'],
            'consumer' => [
                'config' => ['group.id' => 'test-group'],
                'routing' => [
                    ['name' => 'message', 'class' => TestMessage::class],
                ],
            ],
            'producer' => [
                'routing' => [
                    ['name' => 'message', 'topic' => 'test-topic'],
                ],
            ],
        ]);
    }

    public function test_global_validate_schema_is_applied(): void
    {
        $dsn = 'kafka://broker:9092';
        $transportOptions = [
            'transport_name' => 'kafka_test',
            'identifier' => ['staticMethod' => 'getType'],
            'topics' => ['test-topic'],
            'validate_schema' => true,
            'consumer' => [
                'config' => ['group.id' => 'test-group'],
                'routing' => [
                    ['name' => 'message', 'class' => TestMessage::class],
                ],
            ],
            'producer' => [
                'routing' => [
                    ['name' => 'message', 'topic' => 'test-topic'],
                ],
            ],
        ];

        $result = $this->resolver->resolve($dsn, [], $transportOptions);

        $this->assertTrue($result->consumer->validateSchema);
        $this->assertTrue($result->producer->validateSchema);
    }
}
