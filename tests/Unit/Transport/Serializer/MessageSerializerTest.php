<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Tests\Unit\Transport\Serializer;

use Exoticca\KafkaMessenger\Transport\Serializer\MessageSerializer;
use Exoticca\KafkaMessenger\Transport\Stamp\KafkaCustomHeadersStamp;
use Exoticca\KafkaMessenger\Transport\Stamp\KafkaMessageIdentifierStamp;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Stamp\SerializedMessageStamp;

#[CoversClass(MessageSerializer::class)]
#[CoversClass(KafkaMessageIdentifierStamp::class)]
#[CoversClass(KafkaCustomHeadersStamp::class)]
class MessageSerializerTest extends TestCase
{
    private const STATIC_METHOD_IDENTIFIER = 'getType';
    private const TEST_MESSAGE_TYPE = 'test.message';

    private MessageSerializer $serializer;

    protected function setUp(): void
    {
        $routingMap = [
            self::TEST_MESSAGE_TYPE => TestMessage::class,
        ];

        $this->serializer = new MessageSerializer(
            self::STATIC_METHOD_IDENTIFIER,
            $routingMap
        );
    }

    public function test_encode_message_with_identifier(): void
    {
        $message = new TestMessage('Test content');
        $envelope = new Envelope($message);

        $result = $this->serializer->encode($envelope);

        $this->assertArrayHasKey('body', $result);
        $this->assertArrayHasKey('headers', $result);

        $decodedBody = json_decode($result['body'], true);
        $this->assertEquals('Test content', $decodedBody['content']);
        $this->assertEquals(self::TEST_MESSAGE_TYPE, $result['headers'][MessageSerializer::identifierHeaderKey()]);
    }

    public function test_encode_message_with_serialized_message_stamp(): void
    {
        $message = new TestMessage('Test content');
        $serializedContent = '{"content":"Pre-serialized content"}';
        $envelope = new Envelope($message, [
            new SerializedMessageStamp($serializedContent)
        ]);

        $result = $this->serializer->encode($envelope);
        $this->assertEquals($serializedContent, $result['body']);
        $this->assertEquals(self::TEST_MESSAGE_TYPE, $result['headers'][MessageSerializer::identifierHeaderKey()]);
    }

    public function test_encode_message_with_custom_headers(): void
    {
        $message = new TestMessage('Test content');
        $customHeaders = new KafkaCustomHeadersStamp();
        $customHeaders = $customHeaders
            ->withHeader('custom-header-1', 'value1')
            ->withHeader('custom-header-2', 'value2');

        $envelope = new Envelope($message, [$customHeaders]);

        $result = $this->serializer->encode($envelope);

        $this->assertEquals('value1', $result['headers']['custom-header-1']);
        $this->assertEquals('value2', $result['headers']['custom-header-2']);
        $customAttrHeader = json_decode($result['headers'][MessageSerializer::customAttributesHeaderKey()], true);
        $this->assertContains('custom-header-1', $customAttrHeader);
        $this->assertContains('custom-header-2', $customAttrHeader);
    }

    public function test_decode_message_successfully(): void
    {
        $message = new TestMessage('Test content');
        $envelope = new Envelope($message);
        $encodedEnvelope = $this->serializer->encode($envelope);

        $result = $this->serializer->decode($encodedEnvelope);

        $this->assertInstanceOf(TestMessage::class, $result->getMessage());
        $this->assertEquals('Test content', $result->getMessage()->getContent());
        $stamp = $result->last(KafkaMessageIdentifierStamp::class);
        $this->assertEquals(self::TEST_MESSAGE_TYPE, $stamp->identifier);
    }

    public function test_decode_message_with_custom_headers(): void
    {
        $message = new TestMessage('Test content');
        $customHeaders = new KafkaCustomHeadersStamp();
        $customHeaders = $customHeaders
            ->withHeader('custom-header-1', 'value1')
            ->withHeader('custom-header-2', 'value2');

        $envelope = new Envelope($message, [$customHeaders]);
        $encodedEnvelope = $this->serializer->encode($envelope);

        $result = $this->serializer->decode($encodedEnvelope);

        $stamp = $result->last(KafkaCustomHeadersStamp::class);
        $headers = $stamp->getHeaders();
        $this->assertEquals('value1', $headers['custom-header-1']);
        $this->assertEquals('value2', $headers['custom-header-2']);
    }

    public function test_decode_message_with_symfony_headers(): void
    {
        $encodedEnvelope = [
            'body' => json_encode(['content' => 'Test content']),
            'headers' => [
                MessageSerializer::identifierHeaderKey() => self::TEST_MESSAGE_TYPE,
                MessageSerializer::HEADER_SYMFONY_PREFIX . DelayStamp::class => serialize(new DelayStamp(500)),
            ],
        ];

        $result = $this->serializer->decode($encodedEnvelope);
        /** @var DelayStamp $resultStamp */
        $resultStamp = $result->last(DelayStamp::class);
        $this->assertEquals(500, $resultStamp->getDelay());
    }

    public function test_decode_throws_exception_when_message_identifier_missing(): void
    {
        $encodedEnvelope = [
            'body' => json_encode(['content' => 'Test content']),
            'headers' => [],
        ];

        $this->expectException(\Exception::class);
        $this->serializer->decode($encodedEnvelope);
    }

    public function test_decode_throws_exception_when_message_type_not_in_routing_map(): void
    {
        $encodedEnvelope = [
            'body' => json_encode(['content' => 'Test content']),
            'headers' => [
                MessageSerializer::identifierHeaderKey() => 'unknown.message.type',
            ],
        ];

        $this->expectException(\Exception::class);
        $this->serializer->decode($encodedEnvelope);
    }


    public function test_get_reference_name_throws_exception_for_missing_method(): void
    {
        $message = new ClassWithoutTypeMethod();
        $envelope = new Envelope($message);

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Static method "getType" does not exist');

        $this->serializer->encode($envelope);
    }

    public function test_get_reference_name_throws_exception_for_empty_return(): void
    {
        $message = new ClassWithEmptyTypeMethod();
        $envelope = new Envelope($message);

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('must return a non-empty string');

        $this->serializer->encode($envelope);
    }
}

// Test message class
class TestMessage
{
    public function __construct(
        private string $content
    ) {
    }

    public function getContent(): string
    {
        return $this->content;
    }

    public static function getType(): string
    {
        return 'test.message';
    }
}

class ClassWithoutTypeMethod
{
}

class ClassWithEmptyTypeMethod
{
    public static function getType(): string
    {
        return '';
    }
}
