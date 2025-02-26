<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Tests\Unit\SchemaRegistry;

use Exoticca\KafkaMessenger\SchemaRegistry\Avro\AvroSubject;
use Exoticca\KafkaMessenger\SchemaRegistry\SchemaRegistryHttpClient;
use Exoticca\KafkaMessenger\SchemaRegistry\SchemaRegistryManager;
use Exoticca\KafkaMessenger\SchemaRegistry\SchemaRegistrySerializer;
use Exoticca\KafkaMessenger\Tests\ObjectMother\RdKafkaMessageMother;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

#[CoversClass(SchemaRegistryManager::class)]
#[CoversClass(AvroSubject::class)]
class SchemaRegistryManagerTest extends TestCase
{
    private SchemaRegistrySerializer|MockObject $serializer;
    private SchemaRegistryHttpClient|MockObject $httpClient;
    private SchemaRegistryManager|MockObject $manager;

    protected function setUp(): void
    {
        $this->serializer = $this->createMock(SchemaRegistrySerializer::class);
        $this->httpClient = $this->createMock(SchemaRegistryHttpClient::class);
        $this->manager = new SchemaRegistryManager($this->serializer, $this->httpClient);
    }

    public function test_decode(): void
    {
        $this->serializer->expects(
            $this->once()
        )->method('decode');

        $this->httpClient->expects(
            $this->once()
        )->method('getSubjectSchema');

        $this->manager->decode(RdKafkaMessageMother::valid());
    }

    public function test_encode(): void
    {
        $this->serializer->expects(
            $this->once()
        )->method('encode');

        $this->httpClient->expects(
            $this->once()
        )->method('getSubjectSchema');

        $this->manager->encode([], 'topic');
    }
}
