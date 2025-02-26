<?php

namespace Exoticca\KafkaMessenger\SchemaRegistry;

use Avro\SchemaRegistry\AsyncClient;
use Avro\SchemaRegistry\ClientError;
use Avro\SchemaRegistry\Model\Error;
use Avro\Serde;
use Exoticca\KafkaMessenger\SchemaRegistry\Avro\AvroSchema;
use Exoticca\KafkaMessenger\SchemaRegistry\Avro\AvroSubject;
use Safe\Exceptions\JsonException;
use Symfony\Contracts\HttpClient\HttpClientInterface;

class SchemaRegistryHttpClient implements AsyncClient
{
    public const PATH_POST_SCHEMA_REGISTERED = '/subjects/%s';
    public const PATH_POST_REGISTER_SCHEMA = '/subjects/%s/versions';
    public const PATH_GET_SCHEMA = '/schemas/ids/%d';
    public const PATH_GET_SUBJECT_LAST_SCHEMA_VERSION = '/subjects/%s/versions/latest';
    public const PATH_GET_SUBJECT_BY_SCHEMA_VERSION = '/subjects/%s/versions/%s';
    private HttpClientInterface $client;

    public function __construct(
        string $baseUri,
        string $apiKey,
        string $apiSecret,
        HttpClientInterface $httpClient
    ) {
        $this->client = $httpClient->withOptions(
            [
                'base_uri' => $baseUri,
                'headers' => [
                    'Authorization' => 'Basic '.base64_encode(
                        $apiKey.':'.$apiSecret
                    ),
                ],
            ],
        );
    }

    public function getRegisteredSchemaId(string $subject, string $schema): ?int
    {
        try {
            $json = $this->jsonRequest(
                self::PATH_GET_SUBJECT_LAST_SCHEMA_VERSION,
                [$subject]
            );

            return $json['id'];
        } catch (ClientError $e) {
            if (in_array($e->getCode(), [Error::SUBJECT_NOT_FOUND, Error::SCHEMA_NOT_FOUND], true)) {
                return null;
            }

            throw $e;
        }
    }

    public function registerSchema(string $subject, string $schema): int
    {
        $json = $this->jsonRequest(
            self::PATH_POST_REGISTER_SCHEMA,
            [$subject],
            'POST',
            ['schema' => $schema]
        );

        return $json['id'];
    }

    public function getSchema(int $id): string
    {
        $json = $this->jsonRequest(
            self::PATH_GET_SCHEMA,
            [$id]
        );

        return $json['schema'];
    }

    public function getSubjectSchema(AvroSubject $subject, ?int $version = null): AvroSchema
    {
        if ($version) {
            $json = $this->jsonRequest(
                self::PATH_GET_SUBJECT_BY_SCHEMA_VERSION,
                [$version]
            );
        } else {
            $json = $this->jsonRequest(
                self::PATH_GET_SUBJECT_LAST_SCHEMA_VERSION,
                [(string) $subject]
            );
        }

        $schema = Serde::parseSchema($json['schema']);
        $schemaId = $json['id'];
        $version = $json['version'];
        $subject = $json['subject'];

        return new AvroSchema($subject, $schema, $schemaId, $version);
    }

    private function jsonRequest(
        string $path,
        array $params,
        string $method = 'GET',
        array $body = []
    ): array {
        $options = [];
        if (!empty($body)) {
            $options['body'] = $body;
        }
        $response = $this->client->request($method, sprintf($path, ...$params), $options);
        $raw = $response->getContent(false);

        try {
            $json = \Safe\json_decode($raw, true);
            if (Error::isError($json)) {
                throw Error::fromResponse($json);
            }

            return $json;
        } catch (JsonException $e) {
            throw ClientError::jsonParseFailed($raw, $e);
        }
    }
}
