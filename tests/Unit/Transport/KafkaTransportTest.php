<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Tests\Unit\Transport;

use Exoticca\KafkaMessenger\Transport\KafkaTransport;
use Exoticca\KafkaMessenger\Transport\KafkaTransportReceiver;
use Exoticca\KafkaMessenger\Transport\KafkaTransportSender;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;

#[CoversClass(KafkaTransport::class)]
final class KafkaTransportTest extends TestCase
{
    private KafkaTransportSender $sender;
    private KafkaTransportReceiver $receiver;
    private KafkaTransport $transport;
    private Envelope $envelope;

    protected function setUp(): void
    {
        $this->sender = $this->createMock(KafkaTransportSender::class);
        $this->receiver = $this->createMock(KafkaTransportReceiver::class);
        $this->transport = new KafkaTransport($this->sender, $this->receiver);
        $this->envelope = new Envelope(new \stdClass(), [new TransportMessageIdStamp('test-id')]);
    }

    public function test_get_delegates_to_receiver(): void
    {
        $expectedEnvelopes = [$this->envelope];

        $this->receiver->expects($this->once())
            ->method('get')
            ->with()
            ->willReturn($expectedEnvelopes);

        $result = $this->transport->get();

        $this->assertSame($expectedEnvelopes, $result);
    }

    public function test_ack_delegates_to_receiver(): void
    {
        $this->receiver->expects($this->once())
            ->method('ack')
            ->with($this->envelope);

        $this->transport->ack($this->envelope);
    }

    public function test_reject_delegates_to_receiver(): void
    {
        $this->receiver->expects($this->once())
            ->method('reject')
            ->with($this->envelope);

        $this->transport->reject($this->envelope);
    }

    public function test_send_delegates_to_sender(): void
    {
        $sentEnvelope = clone $this->envelope;

        $this->sender->expects($this->once())
            ->method('send')
            ->with($this->envelope)
            ->willReturn($sentEnvelope);

        $result = $this->transport->send($this->envelope);

        $this->assertSame($sentEnvelope, $result);
    }

    public function test_get_from_queues_delegates_to_receiver(): void
    {
        $queueNames = ['topic1', 'topic2'];
        $expectedEnvelopes = [$this->envelope];

        $this->receiver->expects($this->once())
            ->method('get')
            ->with($queueNames)
            ->willReturn($expectedEnvelopes);

        $result = $this->transport->getFromQueues($queueNames);

        $this->assertSame($expectedEnvelopes, $result);
    }
}
