<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Transport;

use RdKafka\Message;
use Symfony\Component\Messenger\Exception\TransportException;

/**
 * Keeps the failed delivery reports of the producer. flush() only tells whether the queue drained in time,
 * a message rejected by the broker (unknown topic, ACLs, too large...) is only reported here.
 */
final class KafkaDeliveryReports
{
    /** @var list<array{err: int, topic: string}> */
    private array $errors = [];

    public function record(Message $message): void
    {
        if (RD_KAFKA_RESP_ERR_NO_ERROR !== $message->err) {
            $this->errors[] = ['err' => $message->err, 'topic' => (string) $message->topic_name];
        }
    }

    /**
     * Throws if any message failed since the last call, and forgets the failures.
     *
     * @throws TransportException
     */
    public function throwIfAnyFailed(): void
    {
        if ([] === $this->errors) {
            return;
        }

        $errors = $this->errors;
        $this->errors = [];

        throw new TransportException(sprintf(
            'Kafka did not deliver %d message(s): %s',
            count($errors),
            implode(', ', array_map(
                fn (array $error) => sprintf('%s (topic "%s")', rd_kafka_err2str($error['err']), $error['topic']),
                $errors,
            )),
        ), $errors[0]['err']);
    }
}
