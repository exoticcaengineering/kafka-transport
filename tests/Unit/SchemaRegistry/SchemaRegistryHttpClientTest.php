<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Tests\Unit\SchemaRegistry;

use Avro\SchemaRegistry\ClientError;
use Avro\SchemaRegistry\Model\Error;
use Exoticca\KafkaMessenger\SchemaRegistry\Avro\AvroSchema;
use Exoticca\KafkaMessenger\SchemaRegistry\Avro\AvroSubject;
use Exoticca\KafkaMessenger\SchemaRegistry\SchemaRegistryHttpClient;
use Exoticca\KafkaMessenger\Tests\ObjectMother\AvroSchemaMother;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpClient\MockHttpClient;
use Symfony\Component\HttpClient\Response\MockResponse;

#[CoversClass(SchemaRegistryHttpClient::class)]
#[CoversClass(AvroSubject::class)]
#[CoversClass(AvroSchema::class)]
class SchemaRegistryHttpClientTest extends TestCase
{
    private array $credentials;

    protected function setUp(): void
    {
        $this->credentials = [
            'base_uri' => 'http://schema-registry.local',
            'api_key' => 'test_key',
            'api_secret' => 'test_secret',
        ];
    }

    public function test_get_registered_schema_id(): void
    {
        $response = new MockResponse(json_encode(['id' => 100091]));
        $client = $this->registryWithCustomResponse($response);

        $schemaId = 100091;

        $subject = 'public_revenue_rule_fct-value';
        $schema = '{}';

        $id = $client->getRegisteredSchemaId($subject, $schema);

        $this->assertEquals($schemaId, $id);
    }

    public function test_register_schema(): void
    {
        $schemaId = 20001;
        $responseBody = json_encode(['id' => $schemaId]);
        $response = new MockResponse($responseBody);
        $client = $this->registryWithCustomResponse($response);

        $subject = 'new_subject';
        $schema = '{"type": "record", "name": "test"}';

        $id = $client->registerSchema($subject, $schema);

        $this->assertEquals($schemaId, $id);
    }

    public function test_get_schema(): void
    {
        $schemaId = 100091;
        $schema = '{"type": "record", "name": "test"}';
        $responseBody = json_encode(['schema' => $schema]);
        $response = new MockResponse($responseBody);
        $client = $this->registryWithCustomResponse($response);

        $retrievedSchema = $client->getSchema($schemaId);

        $this->assertEquals($schema, $retrievedSchema);
    }

    public function test_get_subject_schema(): void
    {
        $response = json_decode(\Safe\file_get_contents(__DIR__.'/../../Fixtures/schema_union.json'), true);
        $response["schema"] = json_encode($response["schema"]);
        $response = new MockResponse(json_encode($response));
        $client = $this->registryWithCustomResponse($response);

        $avroSubject = AvroSubject::ofValue('subject');
        $avroSchema = $client->getSubjectSchema($avroSubject);
        $this->assertEquals($avroSchema, AvroSchemaMother::unionType());
    }

    public function test_get_registered_schema_id_with_not_found(): void
    {
        $responseBody = json_encode([
            'error_code' => Error::SUBJECT_NOT_FOUND,
            'message' => 'Subject not found',
        ]);

        $response = new MockResponse($responseBody);
        $client = $this->registryWithCustomResponse($response);

        $subject = 'non_existent_subject';
        $schema = '{}';

        $id = $client->getRegisteredSchemaId($subject, $schema);
        $this->assertNull($id);
    }

    public function test_json_request_with_invalid_json_response(): void
    {
        $this->expectException(ClientError::class);
        $response = new MockResponse('invalid_json');
        $client = $this->registryWithCustomResponse($response);
        $client->getSchema(100091);
    }

    private function registryWithCustomResponse(MockResponse $response): SchemaRegistryHttpClient
    {
        return new SchemaRegistryHttpClient(
            'http://schema-registry.local',
            'test_key',
            'test_secret',
            new MockHttpClient($response)
        );
    }
}
