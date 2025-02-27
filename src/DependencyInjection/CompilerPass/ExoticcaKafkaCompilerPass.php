<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\DependencyInjection\CompilerPass;

use Exoticca\KafkaMessenger\Transport\KafkaTransportFactory;
use Exoticca\KafkaMessenger\Transport\Metadata\KafkaMetadataHookInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;

class ExoticcaKafkaCompilerPass implements CompilerPassInterface
{
    public function process(ContainerBuilder $container): void
    {
        foreach ($container->findTaggedServiceIds(KafkaMetadataHookInterface::class) as $id => $tags) {
            $definition = $container->getDefinition($id);
            $kafkaTransportDefinition = $container->getDefinition(KafkaTransportFactory::class);
            $kafkaTransportDefinition->replaceArgument(2, $definition);
            return;
        }
    }
}
