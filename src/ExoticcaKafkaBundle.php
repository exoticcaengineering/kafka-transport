<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger;

use Exoticca\KafkaMessenger\SchemaRegistry\AvroSchemaRegistrySerializer;
use Exoticca\KafkaMessenger\SchemaRegistry\SchemaRegistryHttpClient;
use Exoticca\KafkaMessenger\SchemaRegistry\SchemaRegistryManager;
use Exoticca\KafkaMessenger\SchemaRegistry\SchemaRegistrySerializer;
use Exoticca\KafkaMessenger\Transport\Filter\RecordFilterManager;
use Exoticca\KafkaMessenger\Transport\Filter\RecordFilterStrategy;
use Exoticca\KafkaMessenger\Transport\KafkaTransportFactory;
use Exoticca\KafkaMessenger\Transport\KafkaTransportSettingResolver;
use Exoticca\KafkaMessenger\Transport\Setting\SettingManager;
use Symfony\Component\Config\Definition\Configurator\DefinitionConfigurator;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;
use Symfony\Component\DependencyInjection\Reference;
use Symfony\Component\HttpKernel\Bundle\AbstractBundle;
use Symfony\Contracts\HttpClient\HttpClientInterface;

use function Symfony\Component\DependencyInjection\Loader\Configurator\tagged_iterator;

class ExoticcaKafkaBundle extends AbstractBundle
{
    protected string $extensionAlias = 'exoticca_kafka_messenger';

    public function configure(DefinitionConfigurator $definition): void
    {
        $definition->rootNode()
            ->children()
                ->arrayNode('identifier')->addDefaultsIfNotSet()
                    ->info('Schema registry configuration')
                    ->children()
                        ->scalarNode('staticMethod')
                            ->isRequired()
                            ->info('Base URI of the Schema Registry')
                        ->end()
                    ->end()
                ->end()
                ->arrayNode('consumer')->addDefaultsIfNotSet()
                    ->info('Default configuration for Kafka consumers')
                    ->children()
                        ->booleanNode('validate_schema')
                            ->defaultFalse()
                            ->info('Enable or disable schema validation for consumers')
                        ->end()
                        ->booleanNode('commit_async')
                            ->defaultTrue()
                            ->info('Use async commit (true/false)')
                        ->end()
                        ->integerNode('consume_timeout_ms')
                            ->defaultNull()
                            ->info('ConsumerSetting timeout in milliseconds')
                        ->end()
                        ->arrayNode('config')
                            ->info('Kafka consumer configuration')
                            ->variablePrototype()->end()
                        ->end()
                    ->end()
                ->end()

                ->arrayNode('producer')->addDefaultsIfNotSet()
                    ->info('Default configuration for Kafka producers')
                    ->children()
                        ->booleanNode('validate_schema')
                            ->defaultFalse()
                            ->info('Enable or disable schema validation for producers')
                        ->end()
                        ->arrayNode('config')
                            ->info('Kafka producer configuration')
                            ->variablePrototype()->end()
                            ->children()
                                ->scalarNode('client.id')
                                    ->defaultValue('rms')
                                ->end()
                                ->scalarNode('linger.ms')
                                    ->defaultValue('1')
                                ->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()

                ->arrayNode('schema_registry')->addDefaultsIfNotSet()
                    ->info('Schema registry configuration')
                    ->children()
                        ->scalarNode('base_uri')
                            ->defaultNull()
                            ->info('Base URI of the Schema Registry')
                        ->end()
                        ->scalarNode('api_key')
                            ->defaultNull()
                            ->info('API Key of the Schema Registry')
                        ->end()
                        ->scalarNode('api_secret')
                            ->defaultNull()
                            ->info('API Secret of the Schema Registry')
                        ->end()
                    ->end()
                ->end()
                ->scalarNode('serializer')
                    ->defaultNull()
                    ->info('Serializer class to use')
                ->end()
            ->end();
    }

    public function loadExtension(array $config, ContainerConfigurator $container, ContainerBuilder $builder): void
    {
        $services = $container->services();

        $services->set(KafkaTransportSettingResolver::class);

        $services
            ->set(KafkaTransportFactory::class)
            ->args([
                new Reference(KafkaTransportSettingResolver::class),
                new Reference(SchemaRegistryManager::class),
                null
            ])
            ->tag('messenger.transport_factory');

        $services
            ->set(SchemaRegistryManager::class)
            ->args(
                [
                    new Reference(SchemaRegistryHttpClient::class)
                ]
            )->tag('messenger.transport.kafka.exoticca.schema_registry_manager');

        $services->set(SchemaRegistryHttpClient::class)
            ->args([
                $config['schema_registry']["base_uri"],
                $config['schema_registry']["api_key"],
                $config['schema_registry']["api_secret"],
                new Reference(HttpClientInterface::class)
            ])
            ->tag('messenger.transport.kafka.exoticca.schema_registry');

        $kafkaConfigValidator = new SettingManager();
        $kafkaConfigValidator->setupConsumerOptions($config, 'In exoticca_kafka_messenger.consumer configuration');
        $kafkaConfigValidator->setupProducerOptions($config, 'In exoticca_kafka_messenger.producer configuration');

        $kafkaTransportDefinition = $builder->getDefinition(KafkaTransportFactory::class);
        $kafkaTransportDefinition->replaceArgument(2, $config);
    }
}
