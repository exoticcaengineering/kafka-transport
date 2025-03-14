<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Transport\Metadata;

use Symfony\Component\Messenger\Envelope;

interface KafkaMetadataHookInterface
{
    public function beforeProduce(Envelope $envelope): Envelope;
    public function afterConsume(Envelope $envelope): Envelope;
}
