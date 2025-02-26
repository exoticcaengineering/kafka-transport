<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Tests\ObjectMother;

use Exoticca\KafkaMessenger\Transport\Setting\ProducerSetting;

final class ProducerSettingMother
{
    public static function create(array $overrides = []): ProducerSetting
    {
        $defaults = [
            'routing' => ['test.message' => 'test-topic-1'],
            'config' => [],
            'topics' => ['test-topic-1', 'test-topic-2'],
            'pollTimeoutMs' => 0,
            'flushTimeoutMs' => 10000,
            'validateSchema' => false
        ];

        $settings = array_merge($defaults, $overrides);

        return new ProducerSetting(
            $settings['routing'],
            $settings['config'],
            $settings['topics'],
            $settings['pollTimeoutMs'],
            (int) $settings['flushTimeoutMs'],
            (bool) $settings['validateSchema']
        );
    }
}
