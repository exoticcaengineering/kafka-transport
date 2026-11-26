<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Tests\ObjectMother;

use Exoticca\KafkaMessenger\Transport\Setting\CommitMode;
use Exoticca\KafkaMessenger\Transport\Setting\ConsumerSetting;

final class ConsumerSettingMother
{
    public static function create(array $overrides = []): ConsumerSetting
    {
        $defaults = [
            'routing' => ['test.message' => 'TestMessage'],
            'config' => ['group.id' => 'test-group'],
            'topics' => ['test-topic-1', 'test-topic-2'],
            'consumeTimeout' => 500,
            'commitMode' => CommitMode::ASYNC,
            'validateSchema' => false,
            'commitOnReceive' => false,
        ];

        $settings = array_merge($defaults, $overrides);

        return new ConsumerSetting(
            $settings['routing'],
            $settings['config'],
            $settings['topics'],
            (int) $settings['consumeTimeout'],
            $settings['commitMode'],
            (bool) $settings['validateSchema'],
            commitOnReceive: (bool) $settings['commitOnReceive'],
        );
    }
}
