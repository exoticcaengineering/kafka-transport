<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Tests\Unit\Transport\Setting;

use Exoticca\KafkaMessenger\SchemaRegistry\Avro\AvroSubject;
use Exoticca\KafkaMessenger\Transport\Setting\KafkaOptionList;
use Exoticca\KafkaMessenger\Transport\Setting\SettingManager;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(SettingManager::class)]
#[CoversClass(KafkaOptionList::class)]
#[CoversClass(AvroSubject::class)]
class SettingManagerTest extends TestCase
{
    private SettingManager $settingManager;

    protected function setUp(): void
    {
        $this->settingManager = new SettingManager();
    }

    public function test_returns_default_consumer_options_when_no_config_provided(): void
    {
        $result = $this->settingManager->setupConsumerOptions([], 'test context');

        $this->assertIsArray($result);
        $this->assertArrayHasKey('commit_async', $result);
        $this->assertArrayHasKey('consume_timeout_ms', $result);
        $this->assertArrayHasKey('validate_schema', $result);
        $this->assertArrayHasKey('topics', $result);
        $this->assertArrayHasKey('routing', $result);
        $this->assertArrayHasKey('config', $result);

        $this->assertTrue($result['commit_async']);
        $this->assertEquals(500, $result['consume_timeout_ms']);
        $this->assertFalse($result['validate_schema']);
    }

    public function test_returns_default_producer_options_when_no_config_provided(): void
    {
        $result = $this->settingManager->setupProducerOptions([], 'test context');

        $this->assertIsArray($result);
        $this->assertArrayHasKey('validate_schema', $result);
        $this->assertArrayHasKey('poll_timeout_ms', $result);
        $this->assertArrayHasKey('flush_timeout_ms', $result);
        $this->assertArrayHasKey('routing', $result);
        $this->assertArrayHasKey('topics', $result);
        $this->assertArrayHasKey('config', $result);

        $this->assertFalse($result['validate_schema']);
        $this->assertEquals(0, $result['poll_timeout_ms']);
        $this->assertEquals(10000, $result['flush_timeout_ms']);
    }

    public function test_merges_custom_consumer_options_with_defaults(): void
    {
        $config = [
            'consumer' => [
                'commit_async' => false,
                'consume_timeout_ms' => 1000,
                'config' => [
                    'group.id' => 'test-group'
                ]
            ]
        ];

        $result = $this->settingManager->setupConsumerOptions($config, 'test context');

        $this->assertFalse($result['commit_async']);
        $this->assertEquals(1000, $result['consume_timeout_ms']);
        $this->assertEquals('test-group', $result['config']['group.id']);
        $this->assertEquals('earliest', $result['config']['auto.offset.reset']);
    }

    public function test_merges_custom_producer_options_with_defaults(): void
    {
        $config = [
            'producer' => [
                'poll_timeout_ms' => 100,
                'flush_timeout_ms' => 5000,
                'config' => [
                    'client.id' => 'test-client' // Using a valid option from KafkaOptionList
                ]
            ]
        ];

        $result = $this->settingManager->setupProducerOptions($config, 'test context');

        $this->assertEquals(100, $result['poll_timeout_ms']);
        $this->assertEquals(5000, $result['flush_timeout_ms']);
        $this->assertEquals('test-client', $result['config']['client.id']);
    }

    public function test_throws_exception_for_invalid_consumer_option_key(): void
    {
        $config = [
            'consumer' => [
                'invalid_option' => 'value'
            ]
        ];

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid option(s) "invalid_option" test context');

        $this->settingManager->setupConsumerOptions($config, 'test context');
    }

    public function test_throws_exception_for_invalid_producer_option_key(): void
    {
        $config = [
            'producer' => [
                'invalid_option' => 'value'
            ]
        ];

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid option(s) "invalid_option" test context');

        $this->settingManager->setupProducerOptions($config, 'test context');
    }

    public function test_throws_exception_for_invalid_consumer_option_types(): void
    {
        $config = [
            'consumer' => [
                'commit_async' => 'not-a-boolean',
                'config' => [
                    'group.id' => 'test-group'
                ]
            ]
        ];

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('The "commit_async" option type must be boolean');

        $this->settingManager->setupConsumerOptions($config, 'test context');
    }

    public function test_throws_exception_for_invalid_producer_option_types(): void
    {
        $config = [
            'producer' => [
                'poll_timeout_ms' => 'not-an-integer'
            ]
        ];

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('The "poll_timeout_ms" option type must be integer');

        $this->settingManager->setupProducerOptions($config, 'test context');
    }

    public function test_throws_exception_when_consumer_missing_required_config(): void
    {
        $config = [
            'consumer' => [
                'config' => [] // Missing group.id
            ]
        ];

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('The config(s) "group.id" are required');

        $this->settingManager->setupConsumerOptions($config, 'test context');
    }

    public function test_throws_exception_for_invalid_kafka_option(): void
    {
        $config = [
            'consumer' => [
                'config' => [
                    'group.id' => 'test-group',
                    'invalid.kafka.option' => 'value'
                ]
            ]
        ];

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid config option "invalid.kafka.option"');

        $this->settingManager->setupConsumerOptions($config, 'test context');
    }

    public function test_throws_exception_for_non_string_kafka_value(): void
    {
        $config = [
            'consumer' => [
                'config' => [
                    'group.id' => 123 // Not a string
                ]
            ]
        ];

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Kafka config value "group.id" must be a string');

        $this->settingManager->setupConsumerOptions($config, 'test context');
    }

    public function test_throws_exception_for_invalid_consumer_routing(): void
    {
        $config = [
            'consumer' => [
                'routing' => [
                    [
                        'name' => 'route1',
                        // Missing 'class'
                    ]
                ],
                'config' => [
                    'group.id' => 'test-group'
                ]
            ]
        ];

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Each "routing" entry must contain "name" and "class"');

        $this->settingManager->setupConsumerOptions($config, 'test context');
    }

    public function test_throws_exception_for_non_existing_class_in_routing(): void
    {
        $config = [
            'consumer' => [
                'routing' => [
                    [
                        'name' => 'route1',
                        'class' => 'NonExistingClass'
                    ]
                ],
                'config' => [
                    'group.id' => 'test-group'
                ]
            ]
        ];

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('The class "NonExistingClass" specified in "routing" does not exist');

        $this->settingManager->setupConsumerOptions($config, 'test context');
    }

    public function test_throws_exception_for_invalid_producer_routing(): void
    {
        $config = [
            'producer' => [
                'routing' => [
                    [
                        'name' => 'route1',
                        // Missing 'topic'
                    ]
                ]
            ]
        ];

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Each "routing" entry must contain "name" and "topic"');

        $this->settingManager->setupProducerOptions($config, 'test context');
    }

    public function test_handles_valid_consumer_routing_correctly(): void
    {
        eval('namespace Exoticca\TestNamespace; class TestClass {}');

        $config = [
            'consumer' => [
                'routing' => [
                    [
                        'name' => 'route1',
                        'class' => 'Exoticca\TestNamespace\TestClass'
                    ]
                ],
                'config' => [
                    'group.id' => 'test-group'
                ]
            ]
        ];

        $result = $this->settingManager->setupConsumerOptions($config, 'test context');

        $this->assertIsArray($result['routing']);
        $this->assertCount(1, $result['routing']);
        $this->assertEquals('route1', $result['routing'][0]['name']);
        $this->assertEquals('Exoticca\TestNamespace\TestClass', $result['routing'][0]['class']);
    }

    public function test_handles_valid_producer_routing_correctly(): void
    {
        $config = [
            'producer' => [
                'routing' => [
                    [
                        'name' => 'route1',
                        'topic' => 'test-topic'
                    ]
                ]
            ]
        ];

        $result = $this->settingManager->setupProducerOptions($config, 'test context');

        $this->assertIsArray($result['routing']);
        $this->assertCount(1, $result['routing']);
        $this->assertEquals('route1', $result['routing'][0]['name']);
        $this->assertEquals('test-topic', $result['routing'][0]['topic']);
    }

    public function test_validates_consumer_kafka_options_correctly(): void
    {
        $config = [
            'consumer' => [
                'config' => [
                    'group.id' => 'test-group',
                    'auto.offset.reset' => 'latest',
                    'security.protocol' => 'ssl',
                ]
            ]
        ];

        $result = $this->settingManager->setupConsumerOptions($config, 'test context');

        $this->assertEquals('test-group', $result['config']['group.id']);
        $this->assertEquals('latest', $result['config']['auto.offset.reset']);
        $this->assertEquals('ssl', $result['config']['security.protocol']);
    }

    public function test_validates_producer_kafka_options_correctly(): void
    {
        $config = [
            'producer' => [
                'config' => [
                    'acks' => 'all',
                    'client.id' => 'test-producer',
                ]
            ]
        ];

        $result = $this->settingManager->setupProducerOptions($config, 'test context');

        $this->assertEquals('all', $result['config']['acks']);
        $this->assertEquals('test-producer', $result['config']['client.id']);
    }
}
