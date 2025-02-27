<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Tests\Unit\Transport;

use Exoticca\KafkaMessenger\SchemaRegistry\SchemaRegistryManager;
use Exoticca\KafkaMessenger\Tests\ObjectMother\GeneralSettingMother;
use Exoticca\KafkaMessenger\Transport\KafkaConnection;
use Exoticca\KafkaMessenger\Transport\KafkaTransport;
use Exoticca\KafkaMessenger\Transport\KafkaTransportFactory;
use Exoticca\KafkaMessenger\Transport\KafkaTransportReceiver;
use Exoticca\KafkaMessenger\Transport\KafkaTransportSender;
use Exoticca\KafkaMessenger\Transport\KafkaTransportSettingResolver;
use Exoticca\KafkaMessenger\Transport\Serializer\MessageSerializer;
use Exoticca\KafkaMessenger\Transport\Setting\ConsumerSetting;
use Exoticca\KafkaMessenger\Transport\Setting\GeneralSetting;
use Exoticca\KafkaMessenger\Transport\Setting\ProducerSetting;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

#[CoversClass(KafkaTransportFactory::class)]
#[CoversClass(KafkaTransport::class)]
#[CoversClass(KafkaTransportReceiver::class)]
#[CoversClass(KafkaTransportSender::class)]
#[CoversClass(MessageSerializer::class)]
#[CoversClass(ConsumerSetting::class)]
#[CoversClass(GeneralSetting::class)]
#[CoversClass(ProducerSetting::class)]
#[CoversClass(KafkaConnection::class)]
final class KafkaTransportFactoryTest extends TestCase
{
    public function test_supports_kafka_dsn(): void
    {
        $factory = $this->createFactory();

        $this->assertTrue($factory->supports('kafka://localhost:9092', []));
        $this->assertFalse($factory->supports('amqp://localhost:5672', []));
    }

    public function test_create_transport_with_schema_registry(): void
    {
        $generalSetting = GeneralSettingMother::create([
            'transportName' => 'kafka_test',
            'consumer' => [
                'validateSchema' => true
            ],
            'producer' => [
                'validateSchema' => true
            ]
        ]);

        $dsn = 'kafka://localhost:9092';
        $options = ['transport_name' => 'kafka_test'];

        $settingResolver = $this->createMock(KafkaTransportSettingResolver::class);
        $settingResolver->expects($this->once())
            ->method('resolve')
            ->with($dsn, [], $options)
            ->willReturn($generalSetting);

        $serializer = $this->createMock(SerializerInterface::class);
        $schemaRegistry = $this->createMock(SchemaRegistryManager::class);

        $factory = new KafkaTransportFactory(
            $settingResolver,
            $schemaRegistry,
            null,
            []
        );

        $transport = $factory->createTransport($dsn, $options, $serializer);

        $this->assertInstanceOf(KafkaTransport::class, $transport);
    }

    private function createFactory(): KafkaTransportFactory
    {
        $settingResolver = $this->createMock(KafkaTransportSettingResolver::class);

        return new KafkaTransportFactory(
            $settingResolver,
            $this->createMock(SchemaRegistryManager::class),
        );
    }
}
