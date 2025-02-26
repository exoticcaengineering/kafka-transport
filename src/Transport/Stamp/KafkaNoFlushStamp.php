<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Transport\Stamp;

use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

final class KafkaNoFlushStamp implements NonSendableStampInterface
{
}
