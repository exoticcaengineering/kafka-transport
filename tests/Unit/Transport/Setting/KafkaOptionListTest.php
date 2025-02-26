<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Tests\Unit\Transport\Setting;

use Exoticca\KafkaMessenger\Transport\Setting\KafkaOptionList;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(KafkaOptionList::class)]
class KafkaOptionListTest extends TestCase
{
    public function test_global_options_contain_required_keys(): void
    {
        $globalOptions = KafkaOptionList::global();
        $this->assertNotEmpty($globalOptions);
        $this->assertArrayHasKey('client.id', $globalOptions);
        $this->assertArrayHasKey('bootstrap.servers', $globalOptions);
        $this->assertArrayHasKey('security.protocol', $globalOptions);
    }

    public function test_consumer_options_contain_required_keys(): void
    {
        $consumerOptions = KafkaOptionList::consumer();

        $this->assertNotEmpty($consumerOptions);

        $this->assertArrayHasKey('group.id', $consumerOptions);
        $this->assertArrayHasKey('auto.offset.reset', $consumerOptions);
        $this->assertArrayHasKey('enable.auto.commit', $consumerOptions);

        $this->assertArrayHasKey('client.id', $consumerOptions);
        $this->assertArrayHasKey('bootstrap.servers', $consumerOptions);
    }

    public function test_producer_options_contain_required_keys(): void
    {
        $producerOptions = KafkaOptionList::producer();

        $this->assertNotEmpty($producerOptions);

        $this->assertArrayHasKey('acks', $producerOptions);
        $this->assertArrayHasKey('compression.type', $producerOptions);
        $this->assertArrayHasKey('batch.size', $producerOptions);

        $this->assertArrayHasKey('client.id', $producerOptions);
        $this->assertArrayHasKey('bootstrap.servers', $producerOptions);
    }

    public function test_consumer_options_merge_with_global_options(): void
    {
        $globalOptions = KafkaOptionList::global();
        $consumerOptions = KafkaOptionList::consumer();

        foreach (array_keys($globalOptions) as $key) {
            $this->assertArrayHasKey($key, $consumerOptions);
        }

        $this->assertGreaterThan(count($globalOptions), count($consumerOptions));
    }

    public function test_producer_options_merge_with_global_options(): void
    {
        $globalOptions = KafkaOptionList::global();
        $producerOptions = KafkaOptionList::producer();

        foreach (array_keys($globalOptions) as $key) {
            $this->assertArrayHasKey($key, $producerOptions);
        }

        $this->assertGreaterThan(count($globalOptions), count($producerOptions));
    }
}
