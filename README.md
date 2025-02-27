# Exoticca Kafka Messenger Bundle

## What is it for?
The **Exoticca Kafka Messenger Bundle** extends Symfony Messenger's capability to use Kafka as a transport, facilitating integration with event-driven architectures.

---

## Motivation
Existing solutions to integrate Kafka with Symfony Messenger present limitations when working with complex event architectures. This bundle addresses these issues by offering greater flexibility and traceability in event management.

---

## Features
- **Generic Serialization and Deserialization:** Manages the entire message flow using Symfony's serializer.
- **Message Production and Consumption:** Supports both single-topic and multi-topic scenarios, enabling complex event flows.
- **Selective Message Consumption:** Allows consuming only relevant messages for the application while efficiently discarding the rest.
- **Enhanced Traceability:** Supports defining custom headers generically for any message.
- **Optional Schema Validation:** Integrates with *Schema Registry* using Apache Avro, allowing selective validation by transport.
- **Apache Union Schema Compatibility:** Thanks to enhancements on the reference library [avro-php](https://gitlab.com/Jaumo/avro-php).

---

## Installation
```bash
composer require exoticca/kafka-transport
```

Add the bundle to the list of bundles in `config/bundles.php`:
```php
Exoticca\KafkaMessenger\ExoticcaKafkaBundle::class => ['all' => true],
```

---

## Basic Configuration
Create a configuration file at `config/packages/exoticca_kafka_messenger.yaml` with the minimal setup:

```yaml
exoticca_kafka_messenger:
  identifier:
    staticMethod: 'eventName'
  consumer:
    validate_schema: false
    config:
      group.id: 'group-id'
  producer:
    validate_schema: false
    config: {}
```

### Explanation
- `staticMethod`: A static method that returns a string, required in all messages to enable complex Kafka routing.
- `validate_schema`: By default, schema validation against *Schema Registry* is disabled.
- `config`: Allows adding specific Kafka configuration. See the [official documentation](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) for more details.

---

## Transport Configuration
Example of transport configuration in `messenger.yaml`:

```yaml
kafka:
  dsn: '%env(KAFKA_EVENTS_MESSENGER_TRANSPORT_DSN)%'
  options:
    topics: [ 'test_topic' ]
    consumer:
      routing:
        - name: 'test_event'
          class: 'Exoticca\RMS\Domain\Event\TestEvent'
      config:
        group.id: 'group-id'

routing:
  'Exoticca\RMS\Domain\Event\TestEvent': kafka
```

### Execution
To consume the message:
```bash
bin/console messenger:consume kafka -vvv
```
The bundle will automatically deserialize the message.

---

## Advanced Usage
### Consume Multiple Message Types from a Single Topic
If a topic contains multiple event types (e.g., `CustomerRegistered`, `CustomerUpdated`, `CustomerDeleted`), you can configure the bundle to consume only the relevant messages and automatically commit the rest.

### Consume from Multiple Topics in a Single Transport
Example:

```yaml
complex_transport:
  dsn: '%env(KAFKA_EVENTS_MESSENGER_TRANSPORT_DSN)%'
  options:
    topics: ['customers', 'orders']
    consumer:
      config:
        group.id: '%env(APP_ENV)%-rms-public-events'
      routing:
        - name: 'customer_created'
          class: 'Exoticca\Domain\Customer\Event\CustomerCreated'
        - name: 'order_created'
          class: 'Exoticca\Domain\Order\Event\OrderCreated'
```

---

## Acknowledgments
Special thanks to the [avro-php](https://gitlab.com/Jaumo/avro-php) library for facilitating integration with Apache Avro.

