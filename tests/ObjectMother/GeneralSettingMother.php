<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Tests\ObjectMother;

use Exoticca\KafkaMessenger\Transport\Setting\GeneralSetting;

final class GeneralSettingMother
{
    public static function create(array $overrides = []): GeneralSetting
    {
        $defaults = [
            'host' => 'kafka:9092',
            'transportName' => 'kafka',
            'staticMethodIdentifier' => 'getType',
            'producer' => ProducerSettingMother::create(),
            'consumer' => ConsumerSettingMother::create()
        ];

        if (isset($overrides['producer']) && is_array($overrides['producer'])) {
            $overrides['producer'] = ProducerSettingMother::create($overrides['producer']);
        }

        if (isset($overrides['consumer']) && is_array($overrides['consumer'])) {
            $overrides['consumer'] = ConsumerSettingMother::create($overrides['consumer']);
        }

        $settings = array_merge($defaults, $overrides);

        return new GeneralSetting(
            $settings['host'],
            $settings['transportName'],
            $settings['staticMethodIdentifier'],
            $settings['producer'],
            $settings['consumer']
        );
    }
}
