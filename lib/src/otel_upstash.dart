// Licensed under the Apache License, Version 2.0
// Copyright 2025, Mindful Software LLC, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';

import 'otel_upstash_suppression.dart';

const _tracerName = 'dartastic_otel_libs_upstash';

Tracer _tracer() => OTel.tracerProvider().getTracer(_tracerName);

/// Wraps an Upstash Redis command in a CLIENT span using OTel
/// db.* semconv (db.system=redis).
Future<T> tracedUpstashRedis<T>({
  required String command,
  required Future<T> Function() invoke,
  String? key,
  int? argCount,
  String? region,
}) async {
  if (upstashInstrumentationSuppressed()) {
    return invoke();
  }
  final m = <String, Object>{
    'db.system': 'redis',
    'db.operation': command,
    'upstash.service': 'redis',
  };
  if (key != null) m['db.redis.key'] = key;
  if (argCount != null) m['db.redis.arg_count'] = argCount;
  if (region != null) m['cloud.region'] = region;

  final span = _tracer().startSpan(
    'redis $command',
    kind: SpanKind.client,
    attributes: OTel.attributesFromMap(m),
  );
  try {
    return await invoke();
  } catch (e, st) {
    span.addAttributes(OTel.attributes([
      OTel.attributeString(
        ErrorResource.errorType.key,
        e.runtimeType.toString(),
      ),
    ]));
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}

/// Pair returned from `tracedUpstashQstashPublish` callback.
class UpstashQstashResult<T> {
  const UpstashQstashResult({required this.value, this.messageId});
  final T value;
  final String? messageId;
}

/// Wraps an Upstash QStash `publish` in a PRODUCER span.
Future<T> tracedUpstashQstashPublish<T>({
  required String destination,
  required Future<UpstashQstashResult<T>> Function() invoke,
  int? payloadSize,
  String? deduplicationId,
}) async {
  if (upstashInstrumentationSuppressed()) {
    final r = await invoke();
    return r.value;
  }
  final m = <String, Object>{
    'messaging.system': 'upstash_qstash',
    'messaging.operation': 'publish',
    'messaging.destination.name': destination,
    'upstash.service': 'qstash',
  };
  if (payloadSize != null) m['messaging.message.body.size'] = payloadSize;
  if (deduplicationId != null) {
    m['upstash.qstash.deduplication_id'] = deduplicationId;
  }

  final span = _tracer().startSpan(
    'qstash publish $destination',
    kind: SpanKind.producer,
    attributes: OTel.attributesFromMap(m),
  );
  try {
    final r = await invoke();
    if (r.messageId != null) {
      span.addAttributes(OTel.attributes([
        OTel.attributeString('messaging.message.id', r.messageId!),
      ]));
    }
    return r.value;
  } catch (e, st) {
    span.addAttributes(OTel.attributes([
      OTel.attributeString(
        ErrorResource.errorType.key,
        e.runtimeType.toString(),
      ),
    ]));
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}

/// Wraps an Upstash Kafka produce in a PRODUCER span.
Future<T> tracedUpstashKafkaProduce<T>({
  required String topic,
  required Future<T> Function() invoke,
  String? partitionKey,
  int? payloadSize,
}) async {
  if (upstashInstrumentationSuppressed()) {
    return invoke();
  }
  final m = <String, Object>{
    'messaging.system': 'kafka',
    'messaging.operation': 'publish',
    'messaging.destination.name': topic,
    'upstash.service': 'kafka',
  };
  if (partitionKey != null) {
    m['messaging.kafka.message.key'] = partitionKey;
  }
  if (payloadSize != null) m['messaging.message.body.size'] = payloadSize;

  final span = _tracer().startSpan(
    'kafka produce $topic',
    kind: SpanKind.producer,
    attributes: OTel.attributesFromMap(m),
  );
  try {
    return await invoke();
  } catch (e, st) {
    span.addAttributes(OTel.attributes([
      OTel.attributeString(
        ErrorResource.errorType.key,
        e.runtimeType.toString(),
      ),
    ]));
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}
