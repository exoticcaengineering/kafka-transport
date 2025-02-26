<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Transport;

use Exoticca\KafkaMessenger\SchemaRegistry\SchemaRegistryManager;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Exoticca\KafkaMessenger\Transport\Serializer\MessageSerializer;

final readonly class KafkaTransportFactory implements TransportFactoryInterface
{
    private KafkaTransportSettingResolver $configuration;
    private ?array $globalConfig;
    private ?SchemaRegistryManager $schemaRegistryManager;

    public function __construct(
        KafkaTransportSettingResolver $configuration,
        ?SchemaRegistryManager        $schemaRegistryManager = null,
        ?array                        $globalConfig = null,
    ) {
        $this->configuration = $configuration;
        $this->globalConfig = $globalConfig;
        $this->schemaRegistryManager = $schemaRegistryManager;
    }

    public function createTransport(string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        $options = $this->configuration->resolve($dsn, $this->globalConfig, $options);

        $serializer = new MessageSerializer(
            staticMethodIdentifier: $options->staticMethodIdentifier,
            routingMap: $options->consumer->routing,
        );

        $connection = new KafkaConnection(
            generalSetting: $options,
        );

        return new KafkaTransport(
            sender: new KafkaTransportSender(
                connection: $connection,
                serializer: $serializer,
                schemaRegistryManager: $options->producer->validateSchema ? $this->schemaRegistryManager : null,
            ),
            receiver: new KafkaTransportReceiver(
                connection: $connection,
                serializer: $serializer,
                schemaRegistryManager: $options->consumer->validateSchema ? $this->schemaRegistryManager : null,
            )
        );
    }

    public function supports(string $dsn, array $options): bool
    {
        return str_starts_with($dsn, 'kafka://');
    }
}
