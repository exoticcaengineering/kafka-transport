<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Transport\Setting;

enum CommitMode: string
{
    // One blocking commit per message
    case SYNC = 'sync';
    // One commit per message, without waiting for the broker
    case ASYNC = 'async';
    // Offsets are stored in memory and librdkafka commits them every auto.commit.interval.ms
    case STORE = 'store';
}
