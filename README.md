# SQS Connector

A high-level Kotlin library for working with Amazon SQS, built on top of
the [AWS SDK for Kotlin](https://github.com/awslabs/aws-sdk-kotlin) and [Arrow-kt](https://arrow-kt.io/).

## Features

- **Coroutines Based**: Fully asynchronous API using Kotlin Coroutines.
- **Functional Error Handling**: Uses Arrow's `Either` and `Ior` for explicit error handling.
- **Graceful Draining**: Terminate message consumption gracefully, ensuring in-flight messages are processed before
  shutdown.
- **Automatic Visibility Extension**: Automatically extends the visibility timeout of messages currently being
  processed.
- **Batch Processing**: Built-in support for efficient batch sending and deleting of messages.
- **Strongly Typed**: Type-safe representations for Queue URLs, Names, and Receipt Handles.

## Installation

Add the dependency to your `build.gradle.kts`:

```kotlin
dependencies {
    implementation("io.github.jckoenen:sqs-connector:VERSION")
}
```

## Usage

### Initializing SqsConnector

The `SqsConnector` is the main entry point for SQS operations.

```kotlin
val sqsClient = SqsClient { region = "us-east-1" }
val connector = SqsConnector(sqsClient)
```

### Consuming Messages

The library provides a powerful `consume` abstraction that returns a `DrainableFlow`.
This makes it easy to process SQS messages asynchronously.
SQS's visibility timeouts are (optionally) automatically managed so that messages do not expire while they are being
handled.

```kotlin
val consumer = object : MessageConsumer.Individual {
    override val configuration = object : MessageConsumer.Configuration {
        override val parallelism = 5
    }

    override suspend fun handle(message: Message<String>): MessageConsumer.Action {
        println("Processing: ${message.content}")
        return MessageConsumer.Action.DeleteMessage(message)
    }
}

runBlocking {
    val drainControl = connector.consume(Queue.Name("my-queue"), consumer)
        .launchWithDrainControl(this)

    // Later, when you want to shut down gracefully:
    val completed = withTimeoutOrNull(10.seconds) { drainControl.drainAndJoin() }

    // Or terminate, if necessary
    if (completed == null) drainControl.job.cancel()
}
```

## Core Concepts

### Fully Typed Error Handling

Instead of throwing exceptions, this library leverages Arrow's functional types to make error states explicit. Most
operations return an `Either<Failure, Success>`, forcing you to handle potential errors at compile time. For batch
operations, we use a `BatchResult` (based on Arrow's `Ior`), which gracefully handles "partial success" scenarios—common
in SQS when some messages in a batch fail while others succeed.

### Strong Typing

We avoid "stringly-typed" APIs. Identifiers like Queue URLs, Names, and Receipt Handles are wrapped in inline value
classes (e.g., `Queue.Url`, `Message.ReceiptHandle`). This prevents accidental mix-ups.

### Example: Batch Sending Messages

The `sendMessages` API demonstrates both strong typing and typed error handling. It requires a `NonEmptyCollection` of
messages, ensuring you never make an invalid empty batch request.

```kotlin
val queueUrl = Queue.Url("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue")
val messages = nonEmptyListOf(
    OutboundMessage("First message"),
    OutboundMessage("Second message")
)

val result = connector.sendMessages(queueUrl, messages)

result.fold(
    { failures ->
        // Handle failures (Map<SendMessagesFailure, Nel<FailedBatchEntry>>)
        println("Some or all messages failed: $failures")
    },
    { success ->
        // Handle total success (Nel<OutboundMessage>)
        println("All messages sent successfully: $success")
    },
    { failures, success ->
        // Handle partial success
        println("Successfully sent: $success")
        println("Failed: $failures")
    }
)
```

## Development

### Prerequisites

- JDK 17 or higher
- Docker (for running tests via LocalStack)

### Running Tests

Tests use [Testcontainers](https://www.testcontainers.org/) with LocalStack to provide a real SQS environment.

```bash
./gradlew check
```

### Applying Formatting

```bash
./gradlew spotlessApply
```
