// Licensed under the Apache License, Version 2.0
// Copyright 2025, Mindful Software LLC, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';
import 'package:dartastic_opentelemetry/testing.dart';
import 'package:otel_upstash/otel_upstash.dart';
import 'package:test/test.dart';

void main() {
  late TestHarness harness;
  late InMemorySpanExporter spans;

  setUpAll(() async {
    harness = await maybeInitializeOtelForTest(
      serviceName: 'otel_upstash-test',
    );
    spans = harness.spans;
  });

  setUp(() => harness.clear());

  Map<String, Object> attrsOf(Span s) =>
      {for (final a in s.attributes.toList()) a.key: a.value};

  test('Redis GET records CLIENT span with db.* attrs', () async {
    await tracedUpstashRedis<String>(
      command: 'GET',
      key: 'session:42',
      region: 'us-east-1',
      invoke: () async => 'value',
    );
    final span = spans.findSpanByName('redis GET');
    expect(span, isNotNull);
    expect(span!.kind, SpanKind.client);
    final a = attrsOf(span);
    expect(a['db.system'], 'redis');
    expect(a['db.operation'], 'GET');
    expect(a['db.redis.key'], 'session:42');
    expect(a['upstash.service'], 'redis');
    expect(a['cloud.region'], 'us-east-1');
  });

  test('QStash publish records PRODUCER span', () async {
    await tracedUpstashQstashPublish<String>(
      destination: 'https://example.com/handler',
      deduplicationId: 'dedupe-1',
      payloadSize: 200,
      invoke: () async => const UpstashQstashResult(
        value: 'ok',
        messageId: 'm-1',
      ),
    );
    final span =
        spans.findSpanByName('qstash publish https://example.com/handler');
    expect(span, isNotNull);
    expect(span!.kind, SpanKind.producer);
    final a = attrsOf(span);
    expect(a['messaging.system'], 'upstash_qstash');
    expect(a['upstash.qstash.deduplication_id'], 'dedupe-1');
    expect(a['messaging.message.id'], 'm-1');
  });

  test('Kafka produce records PRODUCER span', () async {
    await tracedUpstashKafkaProduce<String>(
      topic: 'orders',
      partitionKey: 'user-1',
      invoke: () async => 'ok',
    );
    final a = attrsOf(spans.findSpanByName('kafka produce orders')!);
    expect(a['messaging.system'], 'kafka');
    expect(a['messaging.destination.name'], 'orders');
    expect(a['messaging.kafka.message.key'], 'user-1');
  });

  test('suppression skips spans', () async {
    await runWithoutUpstashInstrumentationAsync(() async {
      await tracedUpstashRedis<String>(
        command: 'PING',
        invoke: () async => 'pong',
      );
    });
    expect(spans.findSpansStartingWith('redis '), isEmpty);
  });
}
