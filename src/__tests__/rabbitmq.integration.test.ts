import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import test, { type TestContext } from 'node:test';

import { connect } from 'amqplib';
import type { Channel, ChannelModel, ConsumeMessage } from 'amqplib';

import {
  buildConsumerQueueName,
  buildJobBindingKeys,
  edgeFailureCodes,
  buildJobRoutingKey,
  buildRoutingKey,
  checkEventIdentity,
  computeCapabilitiesDigest,
  createCapabilitySnapshot,
  createEdgeChannel,
  defaultExchangeConfig,
  EdgeChannelConnectionManager,
  EdgeEventPublisher,
  EdgeJobConsumer,
  EdgePublishError,
  EdgeStaleDeliveryError,
  EdgeTopologyError,
  type JobDeliveryContext,
  packageInstallResultOutputSchema,
  parseEventRoutingKey,
  parsePackageInstallJob,
  validateEnvelope,
} from '../index.js';
import {
  createAckMessage,
  createBaseEnvelope,
  createExecutionJob,
  createFailureMessage,
  createPackageInstallJob,
  createProgressMessage,
  createResultMessage,
} from './fixtures.js';

const candidateAmqpUrls = [
  process.env.AMQP_INTEGRATION_URL,
  'amqp://mcp:discovery@127.0.0.1:5672',
  'amqp://guest:guest@127.0.0.1:5672',
].filter((value, index, values): value is string => Boolean(value) && values.indexOf(value) === index);

async function connectToBroker(): Promise<{ connection: ChannelModel; amqpUrl: string }> {
  const failures: string[] = [];

  for (const amqpUrl of candidateAmqpUrls) {
    try {
      const connection = await connect(amqpUrl);
      return { connection, amqpUrl };
    } catch (error) {
      failures.push(`${amqpUrl}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  throw new Error(`unable to connect to RabbitMQ using candidate URLs: ${failures.join(' | ')}`);
}

function waitForMessage<T>(
  channel: Channel,
  queueName: string,
  timeoutMs: number,
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    let consumerTag: string | undefined;
    const timeout = setTimeout(() => {
      if (consumerTag) {
        void channel.cancel(consumerTag).catch(() => undefined);
      }

      reject(new Error(`timed out waiting for RabbitMQ message on ${queueName}`));
    }, timeoutMs);

    void channel.consume(
      queueName,
      (message) => {
        if (!message) {
          return;
        }

        clearTimeout(timeout);
        channel.ack(message);
        if (consumerTag) {
          void channel.cancel(consumerTag).catch(() => undefined);
        }
        resolve(JSON.parse(message.content.toString('utf8')) as T);
      },
      { noAck: false },
    ).then((result) => {
      consumerTag = result.consumerTag;
    }).catch((error) => {
      clearTimeout(timeout);
      reject(error);
    });
  });
}

interface IntegrationHarness {
  amqpUrl: string;
  runId: string;
  siteId: string;
  nodeId: string;
  jobsExchange: string;
  eventsExchange: string;
  deadLetterExchange: string;
  consumerQueueName: string;
  controlQueueName: string;
  controlConnection: ChannelModel;
  controlChannel: Channel;
}

async function createHarness(prefix: string): Promise<IntegrationHarness> {
  const runId = crypto.randomUUID().slice(0, 8);
  const siteId = `site-${runId}`;
  const nodeId = `node-${runId}`;
  const jobsExchange = `mcp.edge.jobs.${prefix}.${runId}`;
  const eventsExchange = `mcp.edge.events.${prefix}.${runId}`;
  const deadLetterExchange = `mcp.edge.dlx.${prefix}.${runId}`;
  const consumerQueueName = `mcp.edge.consumer.${siteId}.${nodeId}.${runId}`;
  const controlQueueName = `mcp.edge.control.${siteId}.${nodeId}.${runId}`;
  const { connection: controlConnection, amqpUrl } = await connectToBroker();
  // The broker closes the connection on a refused declare; without a listener that 'error' event
  // is an uncaught exception instead of a test failure.
  controlConnection.on('error', () => undefined);
  try {
    return await declareHarness(controlConnection, amqpUrl, {
      runId, siteId, nodeId, jobsExchange, eventsExchange, deadLetterExchange, consumerQueueName, controlQueueName,
    });
  } catch (error) {
    await controlConnection.close().catch(() => undefined);
    throw error;
  }
}

async function declareHarness(
  controlConnection: ChannelModel,
  amqpUrl: string,
  names: Omit<IntegrationHarness, 'amqpUrl' | 'controlConnection' | 'controlChannel'>,
): Promise<IntegrationHarness> {
  const { runId, siteId, nodeId, jobsExchange, eventsExchange, deadLetterExchange, consumerQueueName, controlQueueName } = names;
  const controlChannel = await controlConnection.createChannel();

  await controlChannel.assertExchange(jobsExchange, 'topic', { durable: true });
  await controlChannel.assertExchange(eventsExchange, 'topic', { durable: true });
  await controlChannel.assertExchange(deadLetterExchange, 'topic', { durable: true });
  // Exclusive, not durable:false + autoDelete: RabbitMQ 4.x refuses transient non-exclusive queues
  // (deprecated feature transient_nonexcl_queues). An exclusive queue is deleted with its connection.
  await controlChannel.assertQueue(controlQueueName, {
    durable: false,
    exclusive: true,
  });
  await controlChannel.bindQueue(
    controlQueueName,
    eventsExchange,
    buildRoutingKey('execution.ack', { siteId, nodeId }).replace('ack', '*'),
  );

  return {
    amqpUrl,
    runId,
    siteId,
    nodeId,
    jobsExchange,
    eventsExchange,
    deadLetterExchange,
    consumerQueueName,
    controlQueueName,
    controlConnection,
    controlChannel,
  };
}

async function destroyHarness(harness: IntegrationHarness): Promise<void> {
  await harness.controlChannel.deleteQueue(harness.controlQueueName).catch(() => undefined);
  for (const exchange of [harness.jobsExchange, harness.eventsExchange, harness.deadLetterExchange]) {
    await harness.controlChannel.deleteExchange(exchange).catch(() => undefined);
  }
  await harness.controlChannel.close().catch(() => undefined);
  await harness.controlConnection.close().catch(() => undefined);
}

test('live RabbitMQ integration publishes job lifecycle events end to end', { timeout: 20000 }, async (t) => {
  const harness = await createHarness('success');
  t.after(() => destroyHarness(harness));

  const edgeChannel = await createEdgeChannel({
    amqpUrl: harness.amqpUrl,
    siteId: harness.siteId,
    nodeId: harness.nodeId,
    runtimeConsumerVersion: '0.1.0',
    runtimeProducerVersion: '0.1.0',
    jobsExchange: harness.jobsExchange,
    eventsExchange: harness.eventsExchange,
    deadLetterExchange: harness.deadLetterExchange,
    consumerQueueName: harness.consumerQueueName,
    consumerQueueOptions: {
      durable: false,
      exclusive: true,
    },
    prefetchCount: 1,
    connectionName: `amqp-mcp-edge-integration-${harness.runId}`,
  });
  t.after(() => edgeChannel.close().catch(() => undefined));

  const ackPromise = waitForMessage(harness.controlChannel, harness.controlQueueName, 15000);
  const progressPromise = waitForMessage(harness.controlChannel, harness.controlQueueName, 15000);
  const resultPromise = waitForMessage(harness.controlChannel, harness.controlQueueName, 15000);

  edgeChannel.consumer.onJob(async (job, delivery) => {
    await edgeChannel.publisher.publishAck(
      createAckMessage({
        messageId: `ack-${harness.runId}`,
        payload: {
          jobId: job.payload.jobId,
          attemptId: job.payload.attemptId,
          siteId: harness.siteId,
          nodeId: harness.nodeId,
          accepted: true,
        },
      }),
    );

    await edgeChannel.publisher.publishProgress(
      createProgressMessage({
        messageId: `progress-${harness.runId}`,
        payload: {
          jobId: job.payload.jobId,
          attemptId: job.payload.attemptId,
          siteId: harness.siteId,
          nodeId: harness.nodeId,
          state: 'running',
          percent: 100,
          metrics: {
            documentsProcessed: 2,
          },
        },
      }),
    );

    await edgeChannel.publisher.publishResult(
      createResultMessage({
        messageId: `result-${harness.runId}`,
        payload: {
          jobId: job.payload.jobId,
          attemptId: job.payload.attemptId,
          siteId: harness.siteId,
          nodeId: harness.nodeId,
          completedAt: new Date().toISOString(),
          output: {
            documents: ['doc-a', 'doc-b'],
          },
        },
      }),
    );

    await edgeChannel.consumer.ack(delivery);
  });

  await edgeChannel.consumer.start();

  const jobEnvelope = createExecutionJob({
    messageId: `job-${harness.runId}`,
    payload: {
      jobId: `job-${harness.runId}`,
      attemptId: `attempt-${harness.runId}`,
      idempotencyKey: `job-${harness.runId}-attempt-${harness.runId}`,
      timeoutMs: 30000,
      target: {
        siteId: harness.siteId,
        nodeId: harness.nodeId,
      },
      createdBy: {
        controlPlane: 'example-control-plane',
      },
      capability: {
        kind: 'tool',
        name: 'search.documents',
      },
      input: {
        query: 'integration smoke test',
      },
    },
  });

  harness.controlChannel.publish(
    harness.jobsExchange,
    buildJobRoutingKey(jobEnvelope.payload.target, jobEnvelope.payload.capability),
    Buffer.from(JSON.stringify(jobEnvelope), 'utf8'),
    {
      contentType: 'application/json',
      type: jobEnvelope.messageType,
      messageId: jobEnvelope.messageId,
    },
  );

  const [ackMessage, progressMessage, resultMessage] = await Promise.all([
    ackPromise,
    progressPromise,
    resultPromise,
  ]);

  const ackEnvelope = validateEnvelope(ackMessage);
  const progressEnvelope = validateEnvelope(progressMessage);
  const resultEnvelope = validateEnvelope(resultMessage);

  assert.equal(ackEnvelope.messageType, 'execution.ack');
  assert.equal(progressEnvelope.messageType, 'execution.progress');
  assert.equal(resultEnvelope.messageType, 'execution.result');
  assert.equal(resultEnvelope.payload.jobId, jobEnvelope.payload.jobId);
});

test('live RabbitMQ integration requeues after nack and publishes failure before success', { timeout: 20000 }, async (t) => {
  const harness = await createHarness('requeue');
  t.after(() => destroyHarness(harness));
  const edgeChannel = await createEdgeChannel({
    amqpUrl: harness.amqpUrl,
    siteId: harness.siteId,
    nodeId: harness.nodeId,
    runtimeConsumerVersion: '0.1.0',
    runtimeProducerVersion: '0.1.0',
    jobsExchange: harness.jobsExchange,
    eventsExchange: harness.eventsExchange,
    deadLetterExchange: harness.deadLetterExchange,
    consumerQueueName: harness.consumerQueueName,
    consumerQueueOptions: {
      durable: false,
      exclusive: true,
    },
    prefetchCount: 1,
    connectionName: `amqp-mcp-edge-requeue-${harness.runId}`,
  });
  t.after(() => edgeChannel.close().catch(() => undefined));

  const failurePromise = waitForMessage(harness.controlChannel, harness.controlQueueName, 15000);
  const ackPromise = waitForMessage(harness.controlChannel, harness.controlQueueName, 15000);
  const resultPromise = waitForMessage(harness.controlChannel, harness.controlQueueName, 15000);
  const deliveries: Array<{ redelivered?: boolean; deliveryTag?: number }> = [];
  let attemptCount = 0;

  edgeChannel.consumer.onJob(async (job, delivery) => {
    attemptCount += 1;
    deliveries.push({
      deliveryTag: delivery.deliveryTag,
      redelivered: delivery.redelivered,
    });

    if (attemptCount === 1) {
      await edgeChannel.publisher.publishFailure(
        createFailureMessage({
          messageId: `failure-${harness.runId}`,
          payload: {
            jobId: job.payload.jobId,
            attemptId: job.payload.attemptId,
            siteId: harness.siteId,
            nodeId: harness.nodeId,
            failedAt: new Date().toISOString(),
            retryable: true,
            code: 'WORKER_RETRY',
            message: 'Synthetic transient failure before requeue',
            details: {
              phase: 'pre-processing',
            },
          },
        }),
      );

      await edgeChannel.consumer.nack(delivery, {
        requeue: true,
        reason: 'retry once',
      });
      return;
    }

    await edgeChannel.publisher.publishAck(
      createAckMessage({
        messageId: `ack-${harness.runId}`,
        payload: {
          jobId: job.payload.jobId,
          attemptId: job.payload.attemptId,
          siteId: harness.siteId,
          nodeId: harness.nodeId,
          accepted: true,
        },
      }),
    );

    await edgeChannel.publisher.publishResult(
      createResultMessage({
        messageId: `result-${harness.runId}`,
        payload: {
          jobId: job.payload.jobId,
          attemptId: job.payload.attemptId,
          siteId: harness.siteId,
          nodeId: harness.nodeId,
          completedAt: new Date().toISOString(),
          output: {
            recovered: true,
            attempts: attemptCount,
          },
        },
      }),
    );

    await edgeChannel.consumer.ack(delivery);
  });

  await edgeChannel.consumer.start();

  const jobEnvelope = createExecutionJob({
    messageId: `job-${harness.runId}`,
    payload: {
      jobId: `job-${harness.runId}`,
      attemptId: `attempt-${harness.runId}`,
      idempotencyKey: `job-${harness.runId}-attempt-${harness.runId}`,
      timeoutMs: 30000,
      target: {
        siteId: harness.siteId,
        nodeId: harness.nodeId,
      },
      createdBy: {
        controlPlane: 'example-control-plane',
      },
      capability: {
        kind: 'tool',
        name: 'search.documents',
      },
      input: {
        query: 'requeue smoke test',
      },
    },
  });

  harness.controlChannel.publish(
    harness.jobsExchange,
    buildJobRoutingKey(jobEnvelope.payload.target, jobEnvelope.payload.capability),
    Buffer.from(JSON.stringify(jobEnvelope), 'utf8'),
    {
      contentType: 'application/json',
      type: jobEnvelope.messageType,
      messageId: jobEnvelope.messageId,
    },
  );

  const [failureMessage, ackMessage, resultMessage] = await Promise.all([
    failurePromise,
    ackPromise,
    resultPromise,
  ]);

  const failureEnvelope = validateEnvelope(failureMessage);
  const ackEnvelope = validateEnvelope(ackMessage);
  const resultEnvelope = validateEnvelope(resultMessage);

  assert.equal(failureEnvelope.messageType, 'execution.failure');
  assert.equal(ackEnvelope.messageType, 'execution.ack');
  assert.equal(resultEnvelope.messageType, 'execution.result');
  assert.equal(deliveries.length, 2);
  assert.equal(deliveries[0]?.redelivered, false);
  assert.equal(deliveries[1]?.redelivered, true);
  assert.equal(resultEnvelope.payload.output.recovered, true);
});

// --- Shared helpers for the topology and node-identity tests -------------------------------------

// Runs cleanup steps in reverse order of registration, after the test, whatever its outcome. Each
// step gets 10 seconds, so a step stuck behind a broken connection cannot hang the run, and the
// steps after it still run.
class CleanupStack {
  private readonly steps: Array<() => Promise<unknown>> = [];

  public constructor(t: TestContext) {
    t.after(async () => {
      for (const step of this.steps.reverse()) {
        let timer: NodeJS.Timeout | undefined;
        const timeout = new Promise<void>((resolve) => {
          timer = setTimeout(resolve, 10_000);
        });
        await Promise.race([step().catch(() => undefined), timeout]);
        clearTimeout(timer);
      }
    });
  }

  public defer(step: () => Promise<unknown>): void {
    this.steps.push(step);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

// Consumes `count` messages from a queue, acks them and returns them in arrival order.
function collectDeliveries(channel: Channel, queueName: string, count: number, timeoutMs: number): Promise<ConsumeMessage[]> {
  return new Promise<ConsumeMessage[]>((resolve, reject) => {
    const received: ConsumeMessage[] = [];
    let consumerTag: string | undefined;
    const finish = (error?: Error) => {
      clearTimeout(timeout);
      if (consumerTag) {
        void channel.cancel(consumerTag).catch(() => undefined);
      }
      if (error) {
        reject(error);
      } else {
        resolve(received);
      }
    };
    const timeout = setTimeout(
      () => finish(new Error(`timed out waiting for ${count} messages on ${queueName}; received ${received.length}`)),
      timeoutMs,
    );

    channel
      .consume(queueName, (message) => {
        if (!message || received.length >= count) {
          return;
        }

        channel.ack(message);
        received.push(message);
        if (received.length === count) {
          finish();
        }
      })
      .then((result) => {
        consumerTag = result.consumerTag;
        if (received.length >= count) {
          void channel.cancel(result.consumerTag).catch(() => undefined);
        }
      })
      .catch((error: Error) => finish(error));
  });
}

async function assertBrokerRefuses(
  connection: ChannelModel,
  label: string,
  operation: (channel: Channel) => Promise<unknown>,
  expected: { code: number; text: RegExp },
): Promise<void> {
  const channel = await connection.createChannel();
  // The refusal closes this channel; without a listener amqplib would close the connection too.
  channel.on('error', () => undefined);
  await assert.rejects(
    () => operation(channel),
    (error: unknown) => {
      assert.equal((error as { code?: unknown }).code, expected.code, label);
      assert.match(String((error as Error).message), expected.text, label);
      return true;
    },
    label,
  );
}

const accessRefused = { code: 403, text: /ACCESS_REFUSED/ };
const notFound = { code: 404, text: /NOT_FOUND/ };

async function withChannel<T>(connection: ChannelModel, operation: (channel: Channel) => Promise<T>): Promise<T> {
  const channel = await connection.createChannel();
  channel.on('error', () => undefined);
  try {
    return await operation(channel);
  } finally {
    await channel.close().catch(() => undefined);
  }
}

function isTopologyError(expected: { kind: 'exchange' | 'queue'; entityName: string; replyCode: number }) {
  return (error: unknown): boolean => {
    assert.ok(error instanceof EdgeTopologyError, `expected an EdgeTopologyError, got ${String(error)}`);
    assert.equal(error.kind, expected.kind);
    assert.equal(error.entityName, expected.entityName);
    assert.equal(error.replyCode, expected.replyCode);
    assert.ok(error.message.includes(`${expected.kind} '${expected.entityName}' does not exist`), error.message);
    return true;
  };
}

// --- Passive topology: what a missing entity does at startup -------------------------------------

test('passive topology: a missing queue or exchange fails startup with a clear error, and nothing is created', { timeout: 20000 }, async (t) => {
  const harness = await createHarness('passive-missing');
  const cleanup = new CleanupStack(t);
  cleanup.defer(() => destroyHarness(harness));

  const config = {
    amqpUrl: harness.amqpUrl,
    siteId: harness.siteId,
    nodeId: harness.nodeId,
    topologyMode: 'passive' as const,
    jobsExchange: harness.jobsExchange,
    eventsExchange: harness.eventsExchange,
    deadLetterExchange: harness.deadLetterExchange,
    consumerQueueName: harness.consumerQueueName,
    connectionName: `amqp-mcp-edge-passive-missing-${harness.runId}`,
  };

  // The node queue was never declared: startup names it, and does not create it.
  await assert.rejects(
    () => createEdgeChannel(config),
    isTopologyError({ kind: 'queue', entityName: harness.consumerQueueName, replyCode: 404 }),
  );
  await assertBrokerRefuses(harness.controlConnection, 'queue was not created', (channel) => channel.checkQueue(harness.consumerQueueName), notFound);

  // The queue exists (durable: the edge worker reads it over another connection), the events
  // exchange does not.
  await harness.controlChannel.assertQueue(harness.consumerQueueName, { durable: true });
  cleanup.defer(() => withChannel(harness.controlConnection, (channel) => channel.deleteQueue(harness.consumerQueueName)));
  const missingEventsExchange = `${harness.eventsExchange}.missing`;

  await assert.rejects(
    () => createEdgeChannel({ ...config, eventsExchange: missingEventsExchange }),
    isTopologyError({ kind: 'exchange', entityName: missingEventsExchange, replyCode: 404 }),
  );
  await assertBrokerRefuses(harness.controlConnection, 'exchange was not created', (channel) => channel.checkExchange(missingEventsExchange), notFound);

  // A failed check closes only its own channel: the connection stays up and nothing starts
  // reconnecting. Once the control plane declares the queue, the same consumer starts.
  const lateQueueName = `${harness.consumerQueueName}.late`;
  const connectionManager = new EdgeChannelConnectionManager({ ...config, consumerQueueName: lateQueueName });
  cleanup.defer(() => connectionManager.close());
  const consumer = new EdgeJobConsumer({ ...config, consumerQueueName: lateQueueName }, connectionManager);

  await assert.rejects(() => consumer.start(), isTopologyError({ kind: 'queue', entityName: lateQueueName, replyCode: 404 }));
  await sleep(300);

  const connectionHealth = await connectionManager.health();
  assert.equal(connectionHealth.connected, true);
  assert.equal(connectionHealth.details?.reconnecting, false);
  assert.equal(connectionHealth.details?.reconnectAttempts, 0);
  const failedHealth = await consumer.health();
  assert.equal(failedHealth.consumerActive, false);
  assert.match(String(failedHealth.details?.topologyError), /does not exist/);

  await harness.controlChannel.assertQueue(lateQueueName, { durable: true });
  cleanup.defer(() => withChannel(harness.controlConnection, (channel) => channel.deleteQueue(lateQueueName)));
  await consumer.start();

  const startedHealth = await consumer.health();
  assert.equal(startedHealth.consumerActive, true);
  assert.equal(startedHealth.details?.topologyError, undefined);
  await consumer.stop();
});

// --- Least privilege: a node user with configure '^$' ---------------------------------------------
//
// These tests create a vhost and a broker user, so they need the management HTTP API and an
// administrator account. They skip, with the reason, when it is unavailable; CI sets
// AMQP_INTEGRATION_REQUIRE_MANAGEMENT=1 so that they fail there instead.

const requireManagement = ['1', 'true'].includes(process.env.AMQP_INTEGRATION_REQUIRE_MANAGEMENT ?? '');

interface ManagementApi {
  baseUrl: string;
  username: string;
  authorization: string;
  // The administrator's AMQP URL, as the suite connected with it.
  amqpUrl: string;
}

let managementApi: Promise<ManagementApi | { unavailable: string }> | undefined;

function decodeUserinfo(value: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

// The management API defaults to port 15672 on the broker host, with the credentials of the AMQP
// URL the suite connected with (in CI and the docker compose file, the broker's default user).
async function discoverManagementApi(): Promise<ManagementApi | { unavailable: string }> {
  const { connection, amqpUrl } = await connectToBroker();
  await connection.close().catch(() => undefined);

  const amqp = new URL(amqpUrl);
  const base = new URL(process.env.AMQP_INTEGRATION_MANAGEMENT_URL ?? `http://${amqp.host.replace(/:\d+$/, '')}:15672`);
  const urlUser = base.username || amqp.username;
  const urlPassword = base.username ? base.password : amqp.password;
  const username = process.env.AMQP_INTEGRATION_MANAGEMENT_USERNAME ?? (urlUser ? decodeUserinfo(urlUser) : 'guest');
  const password = process.env.AMQP_INTEGRATION_MANAGEMENT_PASSWORD ?? (urlUser ? decodeUserinfo(urlPassword) : 'guest');
  base.username = '';
  base.password = '';

  const api: ManagementApi = {
    baseUrl: base.toString().replace(/\/+$/, ''),
    username,
    authorization: `Basic ${Buffer.from(`${username}:${password}`).toString('base64')}`,
    amqpUrl,
  };

  // In CI the management listener can come up after the AMQP one; wait for it there.
  const deadline = Date.now() + (requireManagement ? 20_000 : 0);

  for (;;) {
    const problem = await probeManagementApi(api);

    if (!problem) {
      return api;
    }

    if (Date.now() >= deadline) {
      return {
        unavailable:
          `RabbitMQ management API unavailable at ${api.baseUrl} (${problem}); the least-privilege tests need it ` +
          'to create a vhost and a user. Set AMQP_INTEGRATION_MANAGEMENT_URL, AMQP_INTEGRATION_MANAGEMENT_USERNAME ' +
          'and AMQP_INTEGRATION_MANAGEMENT_PASSWORD to point them at an administrator account.',
      };
    }

    await sleep(1000);
  }
}

async function probeManagementApi(api: ManagementApi): Promise<string | undefined> {
  try {
    const response = await fetch(`${api.baseUrl}/api/whoami`, {
      headers: { authorization: api.authorization },
      signal: AbortSignal.timeout(3000),
    });

    if (!response.ok) {
      return `GET /api/whoami answered ${response.status} for user '${api.username}'`;
    }

    const whoami = (await response.json()) as { name?: string; tags?: string[] | string };
    const tags = Array.isArray(whoami.tags) ? whoami.tags : String(whoami.tags ?? '').split(',');

    return tags.includes('administrator') ? undefined : `user '${api.username}' is not an administrator`;
  } catch (error) {
    return error instanceof Error ? error.message : String(error);
  }
}

async function managementApiOrSkip(t: TestContext): Promise<ManagementApi | undefined> {
  managementApi ??= discoverManagementApi();
  const api = await managementApi;

  if ('unavailable' in api) {
    if (requireManagement) {
      throw new Error(`${api.unavailable} AMQP_INTEGRATION_REQUIRE_MANAGEMENT is set, so this test fails instead of skipping.`);
    }

    t.skip(api.unavailable);
    return undefined;
  }

  return api;
}

async function managementRequest(api: ManagementApi, method: 'PUT' | 'DELETE', path: string, body?: unknown): Promise<void> {
  const response = await fetch(`${api.baseUrl}${path}`, {
    method,
    headers: { authorization: api.authorization, 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: AbortSignal.timeout(10_000),
  });

  if (!response.ok && !(method === 'DELETE' && response.status === 404)) {
    throw new Error(`management API ${method} ${path} answered ${response.status}: ${await response.text()}`);
  }
}

async function managementGet<T>(api: ManagementApi, path: string): Promise<T> {
  const response = await fetch(`${api.baseUrl}${path}`, {
    headers: { authorization: api.authorization },
    signal: AbortSignal.timeout(10_000),
  });

  if (!response.ok) {
    throw new Error(`management API GET ${path} answered ${response.status}: ${await response.text()}`);
  }

  return (await response.json()) as T;
}

async function waitFor<T>(label: string, timeoutMs: number, probe: () => Promise<T | undefined>): Promise<T> {
  const deadline = Date.now() + timeoutMs;

  for (;;) {
    const value = await probe();

    if (value !== undefined) {
      return value;
    }

    if (Date.now() >= deadline) {
      throw new Error(`timed out after ${timeoutMs}ms waiting for ${label}`);
    }

    await sleep(200);
  }
}

function brokerUrl(baseAmqpUrl: string, vhost: string, credentials?: { username: string; password: string }): string {
  const url = new URL(baseAmqpUrl);
  if (credentials) {
    url.username = encodeURIComponent(credentials.username);
    url.password = encodeURIComponent(credentials.password);
  }
  url.pathname = `/${encodeURIComponent(vhost)}`;
  return url.toString();
}

interface LeastPrivilegeHarness {
  cleanup: CleanupStack;
  runId: string;
  siteId: string;
  nodeId: string;
  vhost: string;
  adminUser: string;
  nodeUser: string;
  nodeAmqpUrl: string;
  queueName: string;
  deadLetterQueueName: string;
  controlQueueName: string;
  controlConnection: ChannelModel;
  controlChannel: Channel;
}

// Builds, in a vhost of its own, what the platform provisions for one edge node: the shared
// exchanges under their production names, the node queue with its dead-letter exchange and
// bindings, a dead-letter queue, and a broker user with the least-privilege permission set from
// docs/PROTOCOL.md. Everything is removed after the test.
async function createLeastPrivilegeHarness(
  t: TestContext,
  api: ManagementApi,
  options: { queueType?: 'classic' | 'quorum' } = {},
): Promise<LeastPrivilegeHarness> {
  const cleanup = new CleanupStack(t);
  const runId = crypto.randomUUID().slice(0, 8);
  const siteId = `site-${runId}`;
  const nodeId = `node-${runId}`;
  const vhost = `amqp-mcp-edge-it-${runId}`;
  const vhostPath = encodeURIComponent(vhost);
  // The '@' must survive URL-encoding in amqpUrl and decoding into userId, or the broker refuses
  // every event the node publishes.
  const nodeUser = `${nodeId}@${siteId}`;
  const nodeUserPath = encodeURIComponent(nodeUser);
  const nodePassword = crypto.randomUUID();
  const { jobsExchange, eventsExchange, deadLetterExchange } = defaultExchangeConfig;
  const nodeConfig = { amqpUrl: 'amqp://unused', siteId, nodeId };
  const queueName = buildConsumerQueueName(nodeConfig);
  const deadLetterQueueName = `mcp.edge.dlq.${siteId}.${nodeId}`;
  const controlQueueName = `mcp.edge.control.${siteId}.${nodeId}`;

  await managementRequest(api, 'PUT', `/api/vhosts/${vhostPath}`, {});
  cleanup.defer(() => managementRequest(api, 'DELETE', `/api/vhosts/${vhostPath}`));
  await managementRequest(api, 'PUT', `/api/permissions/${vhostPath}/${encodeURIComponent(api.username)}`, {
    configure: '.*',
    write: '.*',
    read: '.*',
  });
  cleanup.defer(() => managementRequest(api, 'DELETE', `/api/permissions/${vhostPath}/${encodeURIComponent(api.username)}`));

  // The control plane's side of the broker.
  const controlConnection = await connect(brokerUrl(api.amqpUrl, vhost));
  controlConnection.on('error', () => undefined);
  cleanup.defer(() => controlConnection.close());
  const controlChannel = await controlConnection.createChannel();
  controlChannel.on('error', () => undefined);

  for (const exchange of [jobsExchange, eventsExchange, deadLetterExchange]) {
    await controlChannel.assertExchange(exchange, 'topic', { durable: true });
  }

  // Durable, not exclusive: the node reads it over its own connection, and RabbitMQ 4 refuses
  // transient non-exclusive queues.
  await controlChannel.assertQueue(queueName, {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': deadLetterExchange,
      ...(options.queueType ? { 'x-queue-type': options.queueType } : {}),
    },
  });
  cleanup.defer(() => withChannel(controlConnection, (channel) => channel.deleteQueue(queueName)));
  for (const bindingKey of buildJobBindingKeys(nodeConfig)) {
    await controlChannel.bindQueue(queueName, jobsExchange, bindingKey);
  }

  await controlChannel.assertQueue(deadLetterQueueName, { durable: true });
  cleanup.defer(() => withChannel(controlConnection, (channel) => channel.deleteQueue(deadLetterQueueName)));
  await controlChannel.bindQueue(deadLetterQueueName, deadLetterExchange, '#');

  await controlChannel.assertQueue(controlQueueName, { durable: false, exclusive: true });
  cleanup.defer(() => withChannel(controlConnection, (channel) => channel.deleteQueue(controlQueueName)));
  await controlChannel.bindQueue(controlQueueName, eventsExchange, `edge.event.*.${siteId}.${nodeId}`);

  // The node's broker user.
  await managementRequest(api, 'PUT', `/api/users/${nodeUserPath}`, { password: nodePassword, tags: '' });
  cleanup.defer(() => managementRequest(api, 'DELETE', `/api/users/${nodeUserPath}`));
  await managementRequest(api, 'PUT', `/api/permissions/${vhostPath}/${nodeUserPath}`, {
    configure: '^$',
    write: `^${escapeRegExp(eventsExchange)}$`,
    read: `^${escapeRegExp(queueName)}$`,
  });
  cleanup.defer(() => managementRequest(api, 'DELETE', `/api/permissions/${vhostPath}/${nodeUserPath}`));
  await managementRequest(api, 'PUT', `/api/topic-permissions/${vhostPath}/${nodeUserPath}`, {
    exchange: eventsExchange,
    write: `^edge\\.event\\.[a-z]+\\.${escapeRegExp(siteId)}\\.${escapeRegExp(nodeId)}$`,
    read: '^$',
  });
  cleanup.defer(() => managementRequest(api, 'DELETE', `/api/topic-permissions/${vhostPath}/${nodeUserPath}`));

  return {
    cleanup,
    runId,
    siteId,
    nodeId,
    vhost,
    adminUser: api.username,
    nodeUser,
    nodeAmqpUrl: brokerUrl(api.amqpUrl, vhost, { username: nodeUser, password: nodePassword }),
    queueName,
    deadLetterQueueName,
    controlQueueName,
    controlConnection,
    controlChannel,
  };
}

test('least privilege: a passive-mode worker with configure \'^$\' runs jobs and events end to end', { timeout: 60000 }, async (t) => {
  const api = await managementApiOrSkip(t);
  if (!api) {
    return;
  }

  const harness = await createLeastPrivilegeHarness(t, api);
  const { jobsExchange } = defaultExchangeConfig;

  // Production settings: passive topology and the default exchange and queue names.
  const edgeChannel = await createEdgeChannel({
    amqpUrl: harness.nodeAmqpUrl,
    siteId: harness.siteId,
    nodeId: harness.nodeId,
    topologyMode: 'passive',
    runtimeConsumerVersion: '0.1.0',
    runtimeProducerVersion: '0.1.0',
    connectionName: `amqp-mcp-edge-least-privilege-${harness.runId}`,
  });
  harness.cleanup.defer(() => edgeChannel.close());

  const eventsReceived = collectDeliveries(harness.controlChannel, harness.controlQueueName, 3, 15000);

  edgeChannel.consumer.onJob(async (job, delivery) => {
    const common = { jobId: job.payload.jobId, attemptId: job.payload.attemptId, siteId: harness.siteId, nodeId: harness.nodeId };
    await edgeChannel.publisher.publishAck(createAckMessage({ messageId: `ack-${harness.runId}`, payload: { ...common, accepted: true } }));
    await edgeChannel.publisher.publishProgress(
      createProgressMessage({ messageId: `progress-${harness.runId}`, payload: { ...common, state: 'running', percent: 100 } }),
    );
    await edgeChannel.publisher.publishResult(
      createResultMessage({
        messageId: `result-${harness.runId}`,
        payload: { ...common, completedAt: new Date().toISOString(), output: { ok: true } },
      }),
    );
    await edgeChannel.consumer.ack(delivery);
  });

  await edgeChannel.consumer.start();

  const jobEnvelope = createExecutionJob({
    messageId: `job-${harness.runId}`,
    payload: {
      ...createExecutionJob().payload,
      jobId: `job-${harness.runId}`,
      attemptId: `attempt-${harness.runId}`,
      idempotencyKey: `job-${harness.runId}-attempt-1`,
      target: { siteId: harness.siteId, nodeId: harness.nodeId },
    },
  });
  const jobRoutingKey = buildJobRoutingKey(jobEnvelope.payload.target, jobEnvelope.payload.capability);
  harness.controlChannel.publish(jobsExchange, jobRoutingKey, Buffer.from(JSON.stringify(jobEnvelope), 'utf8'), {
    contentType: 'application/json',
    type: jobEnvelope.messageType,
    messageId: jobEnvelope.messageId,
    userId: harness.adminUser,
  });

  const events = await eventsReceived;

  assert.deepEqual(
    events.map((message) => message.fields.routingKey),
    ['ack', 'progress', 'result'].map((type) => `edge.event.${type}.${harness.siteId}.${harness.nodeId}`),
  );
  for (const message of events) {
    // Every event carries the node's broker username, which the broker has validated.
    assert.equal(message.properties.userId, harness.nodeUser);
    const envelope = validateEnvelope(JSON.parse(message.content.toString('utf8')));
    assert.ok(envelope.messageType.startsWith('execution.') && 'jobId' in envelope.payload);
    assert.equal(envelope.payload.jobId, jobEnvelope.payload.jobId);
  }

  // Dead-lettering needs no permission from the node: a job it rejects reaches the dead-letter
  // queue the platform bound to mcp.edge.dlx.
  const deadLettered = collectDeliveries(harness.controlChannel, harness.deadLetterQueueName, 1, 15000);
  harness.controlChannel.publish(jobsExchange, jobRoutingKey, Buffer.from('not json', 'utf8'), {
    contentType: 'application/json',
    messageId: `malformed-${harness.runId}`,
    userId: harness.adminUser,
  });

  const [deadLetter] = await deadLettered;
  assert.equal(deadLetter?.properties.messageId, `malformed-${harness.runId}`);
  assert.equal(deadLetter?.content.toString('utf8'), 'not json');
  const death = (deadLetter?.properties.headers?.['x-death'] as Array<{ queue?: string; reason?: string }> | undefined)?.[0];
  assert.equal(death?.queue, harness.queueName);
  assert.equal(death?.reason, 'rejected');
});

test('least privilege: the worker user cannot delete or redeclare the shared topology', { timeout: 60000 }, async (t) => {
  const api = await managementApiOrSkip(t);
  if (!api) {
    return;
  }

  const harness = await createLeastPrivilegeHarness(t, api);
  const { jobsExchange, eventsExchange, deadLetterExchange } = defaultExchangeConfig;
  const node = await connect(harness.nodeAmqpUrl);
  node.on('error', () => undefined);
  harness.cleanup.defer(() => node.close());

  await assertBrokerRefuses(node, `delete ${jobsExchange}`, (channel) => channel.deleteExchange(jobsExchange), accessRefused);
  await assertBrokerRefuses(node, `delete ${eventsExchange}`, (channel) => channel.deleteExchange(eventsExchange), accessRefused);
  await assertBrokerRefuses(node, `delete ${deadLetterExchange}`, (channel) => channel.deleteExchange(deadLetterExchange), accessRefused);
  await assertBrokerRefuses(node, 'delete its own queue', (channel) => channel.deleteQueue(harness.queueName), accessRefused);
  await assertBrokerRefuses(
    node,
    `declare ${eventsExchange}`,
    (channel) => channel.assertExchange(eventsExchange, 'topic', { durable: true }),
    accessRefused,
  );
  await assertBrokerRefuses(
    node,
    'bind its own queue to every job',
    (channel) => channel.bindQueue(harness.queueName, jobsExchange, 'edge.job.#'),
    accessRefused,
  );

  // Passive declares need no configure permission: the checks passive mode makes succeed.
  await withChannel(node, (channel) => channel.checkQueue(harness.queueName));
  await withChannel(node, (channel) => channel.checkExchange(eventsExchange));

  // The refusals closed only their own channels, and the shared exchanges are all still there.
  assert.equal(await withChannel(node, async () => 'connection still open'), 'connection still open');
  for (const exchange of [jobsExchange, eventsExchange, deadLetterExchange]) {
    await withChannel(harness.controlConnection, (channel) => channel.checkExchange(exchange));
  }

  // The default declare mode cannot start under this user: it needs configure.
  const declaring = await createEdgeChannel({
    amqpUrl: harness.nodeAmqpUrl,
    siteId: harness.siteId,
    nodeId: harness.nodeId,
    topologyMode: 'declare',
    reconnectEnabled: false,
  });
  harness.cleanup.defer(() => declaring.close());
  await assert.rejects(() => declaring.consumer.start(), /ACCESS_REFUSED/);
});

test('least privilege: an event with a forged user_id closes the channel with PRECONDITION_FAILED', { timeout: 60000 }, async (t) => {
  const api = await managementApiOrSkip(t);
  if (!api) {
    return;
  }

  const harness = await createLeastPrivilegeHarness(t, api);
  const { eventsExchange } = defaultExchangeConfig;
  const routingKey = buildRoutingKey('execution.ack', { siteId: harness.siteId, nodeId: harness.nodeId });
  const node = await connect(harness.nodeAmqpUrl);
  node.on('error', () => undefined);
  harness.cleanup.defer(() => node.close());

  const forgingChannel = await node.createChannel();
  const channelError = new Promise<Error & { code?: number }>((resolve, reject) => {
    forgingChannel.once('error', resolve);
    setTimeout(() => reject(new Error('the broker accepted an event with a forged user_id')), 5000);
  });
  forgingChannel.publish(eventsExchange, routingKey, Buffer.from(JSON.stringify(createAckMessage()), 'utf8'), {
    contentType: 'application/json',
    messageId: 'forged',
    userId: 'another-node',
  });

  const error = await channelError;
  assert.equal(error.code, 406);
  assert.match(error.message, /PRECONDITION_FAILED/);
  assert.match(error.message, new RegExp(`user_id property set to 'another-node' but authenticated user was '${escapeRegExp(harness.nodeUser)}'`));

  // The broker checks user_id only when it is set: an event without one is delivered. That is why
  // consumers must reject events that lack it. The forged event, published first, never arrived.
  const received = collectDeliveries(harness.controlChannel, harness.controlQueueName, 2, 10000);
  await withChannel(node, async (channel) => {
    channel.publish(eventsExchange, routingKey, Buffer.from('{}'), { messageId: 'genuine', userId: harness.nodeUser });
    channel.publish(eventsExchange, routingKey, Buffer.from('{}'), { messageId: 'anonymous' });
    // A passive check round-trips, so both publishes have reached the broker before the close.
    await channel.checkExchange(eventsExchange);
  });

  const [genuine, anonymous] = await received;
  assert.equal(genuine?.properties.messageId, 'genuine');
  assert.equal(genuine?.properties.userId, harness.nodeUser);
  assert.equal(anonymous?.properties.messageId, 'anonymous');
  assert.equal(anonymous?.properties.userId, undefined);
});


test('least privilege: after a reconnect a passive consumer whose queue is gone stops, without a reconnect loop', { timeout: 60000 }, async (t) => {
  const api = await managementApiOrSkip(t);
  if (!api) {
    return;
  }

  const harness = await createLeastPrivilegeHarness(t, api);
  const connectionName = `amqp-mcp-edge-reconnect-${harness.runId}`;
  const edgeChannel = await createEdgeChannel({
    amqpUrl: harness.nodeAmqpUrl,
    siteId: harness.siteId,
    nodeId: harness.nodeId,
    topologyMode: 'passive',
    // Long enough to delete the queue while the node is disconnected.
    reconnectDelayMs: 1000,
    connectionName,
  });
  harness.cleanup.defer(() => edgeChannel.close());
  await edgeChannel.consumer.start();

  // The node's connection, as the broker lists it.
  const listConnections = () =>
    managementGet<Array<{ name: string; client_properties?: { connection_name?: string } }>>(
      api,
      `/api/vhosts/${encodeURIComponent(harness.vhost)}/connections`,
    ).then((connections) => connections.filter((connection) => connection.client_properties?.connection_name === connectionName));
  const [original] = await waitFor('the node connection to be listed', 15000, async () => {
    const connections = await listConnections();
    return connections.length === 1 ? connections : undefined;
  });

  // The broker drops the connection, and the queue disappears while the node is disconnected. (A
  // queue deleted while the node is connected is the broker-cancel test below.)
  await managementRequest(api, 'DELETE', `/api/connections/${encodeURIComponent(original!.name)}`);
  await waitFor('the node to see the connection drop', 5000, async () => {
    const health = await edgeChannel.consumer.health();
    return health.connected === false ? health : undefined;
  });
  await harness.controlChannel.deleteQueue(harness.queueName);

  const stopped = await waitFor('the consumer to stop', 15000, async () => {
    const health = await edgeChannel.consumer.health();
    return health.connected && health.consumerActive === false && health.details?.topologyError !== undefined ? health : undefined;
  });
  assert.match(String(stopped.details?.topologyError), new RegExp(`queue '${escapeRegExp(harness.queueName)}' does not exist`));
  // It stopped on the reconnect's check, not on a broker cancel.
  assert.equal(stopped.details?.brokerCancellations, undefined);

  // One new connection, still up: the failed check closed only its own channel.
  await sleep(1000);
  const health = await edgeChannel.consumer.health();
  assert.equal(health.connected, true);
  assert.equal(health.consumerActive, false);
  const replacements = await waitFor('the replacement connection to be listed', 15000, async () => {
    const connections = await listConnections();
    return connections.length === 1 && connections[0]?.name !== original!.name ? connections : undefined;
  });
  assert.equal(replacements.length, 1);

  // The publisher, on the same new connection, checks the events exchange again and still works.
  const received = collectDeliveries(harness.controlChannel, harness.controlQueueName, 1, 10000);
  await edgeChannel.publisher.publishFailure(
    createFailureMessage({
      messageId: `failure-${harness.runId}`,
      payload: {
        ...createFailureMessage().payload,
        siteId: harness.siteId,
        nodeId: harness.nodeId,
        code: 'QUEUE_MISSING',
        message: 'consumer stopped: its queue is gone',
      },
    }),
  );
  const [event] = await received;
  assert.equal(event?.properties.messageId, `failure-${harness.runId}`);
  assert.equal(event?.properties.userId, harness.nodeUser);
});

test('least privilege: node.hello, node.heartbeat and a package.install job round trip under the node permission set', { timeout: 60000 }, async (t) => {
  const api = await managementApiOrSkip(t);
  if (!api) {
    return;
  }

  const harness = await createLeastPrivilegeHarness(t, api);
  const { jobsExchange } = defaultExchangeConfig;
  const edgeChannel = await createEdgeChannel({
    amqpUrl: harness.nodeAmqpUrl,
    siteId: harness.siteId,
    nodeId: harness.nodeId,
    topologyMode: 'passive',
    runtimeConsumerVersion: '0.1.0',
    runtimeProducerVersion: '0.1.0',
    connectionName: `amqp-mcp-edge-lifecycle-${harness.runId}`,
  });
  harness.cleanup.defer(() => edgeChannel.close());

  const node = { siteId: harness.siteId, nodeId: harness.nodeId };
  const lifecycle = { ...node, sessionId: `session-${harness.runId}`, heartbeatIntervalMs: 30000 };
  const distHash = `mcpod-dist-v2:${'a'.repeat(64)}`;
  const release = { packageId: 'net-tools', packageVersion: '1.2.0', releaseId: `rel-${harness.runId}` };
  let snapshot = createCapabilitySnapshot();

  // The node: hello, then a deploy job, then a heartbeat carrying the changed snapshot.
  edgeChannel.consumer.onJob(async (job, delivery) => {
    const install = parsePackageInstallJob(job);
    const common = { jobId: job.payload.jobId, attemptId: job.payload.attemptId, ...node };
    await edgeChannel.publisher.publishAck(createAckMessage({ messageId: `ack-${harness.runId}`, payload: { ...common, accepted: true } }));
    snapshot = createCapabilitySnapshot({ packages: [{ ...install.release, distHash: install.distHash }] });
    await edgeChannel.publisher.publishResult(
      createResultMessage({
        messageId: `result-${harness.runId}`,
        payload: { ...common, completedAt: new Date().toISOString(), output: { release: install.release, distHash: install.distHash, alreadyInstalled: false } },
      }),
    );
    await edgeChannel.consumer.ack(delivery);
    await edgeChannel.publisher.publishNodeHeartbeat({
      ...createBaseEnvelope(),
      messageType: 'node.heartbeat',
      messageId: `heartbeat-${harness.runId}`,
      payload: { ...lifecycle, sequence: 2, capabilities: snapshot, capabilitiesDigest: computeCapabilitiesDigest(snapshot) },
    });
  });
  await edgeChannel.consumer.start();

  const received = collectDeliveries(harness.controlChannel, harness.controlQueueName, 4, 15000);
  await edgeChannel.publisher.publishNodeHello({
    ...createBaseEnvelope(),
    messageType: 'node.hello',
    messageId: `hello-${harness.runId}`,
    payload: { ...lifecycle, sequence: 1, startedAt: new Date().toISOString(), capabilities: snapshot, capabilitiesDigest: computeCapabilitiesDigest(snapshot) },
  });

  // The control plane: a deploy job for this node.
  const base = createPackageInstallJob();
  const job = createPackageInstallJob(
    { release },
    {
      messageId: `install-${harness.runId}`,
      payload: { ...base.payload, jobId: `install-${harness.runId}`, attemptId: `install-${harness.runId}-1`, target: node, input: { ...base.payload.input, release, distHash } },
    },
  );
  harness.controlChannel.publish(jobsExchange, buildJobRoutingKey(job.payload.target, job.payload.capability), Buffer.from(JSON.stringify(job), 'utf8'), {
    contentType: 'application/json',
    type: job.messageType,
    messageId: job.messageId,
    userId: harness.adminUser,
  });

  const events = await received;
  const provisioned = { userId: harness.nodeUser, ...node };
  const parsed = events.map((message) => {
    const envelope = validateEnvelope(JSON.parse(message.content.toString('utf8')));
    // What every consumer of the events exchange checks: the broker-validated user_id and routing key.
    assert.deepEqual(checkEventIdentity({ routingKey: message.fields.routingKey, userId: message.properties.userId, envelope }, provisioned), []);
    assert.equal(parseEventRoutingKey(message.fields.routingKey)?.messageType, envelope.messageType);
    return envelope;
  });

  assert.deepEqual(parsed.map((envelope) => envelope.messageType), ['node.hello', 'execution.ack', 'execution.result', 'node.heartbeat']);
  const [hello, , result, heartbeat] = parsed;
  assert.ok(hello?.messageType === 'node.hello' && result?.messageType === 'execution.result' && heartbeat?.messageType === 'node.heartbeat');
  assert.deepEqual(hello.payload.capabilities.packages, []);
  assert.equal(packageInstallResultOutputSchema.parse(result.payload.output).release.releaseId, release.releaseId);
  assert.deepEqual(heartbeat.payload.capabilities?.packages, [{ ...release, distHash }]);
  assert.notEqual(heartbeat.payload.capabilitiesDigest, hello.payload.capabilitiesDigest);
  assert.equal(heartbeat.payload.sessionId, hello.payload.sessionId);
});

// The classic run uses the default failure report (no error text); the quorum run opts in to a
// description, which must arrive cut to 1000 characters.
for (const queueType of ['classic', 'quorum'] as const) {
  const describes = queueType === 'quorum';
  test(`poison job on a ${queueType} queue: delivered maxDeliveryAttempts times, reported, dead-lettered, and the next job runs`, { timeout: 60000 }, async (t) => {
    const api = await managementApiOrSkip(t);
    if (!api) {
      return;
    }

    const harness = await createLeastPrivilegeHarness(t, api, { queueType });
    const { jobsExchange } = defaultExchangeConfig;
    const edgeChannel = await createEdgeChannel({
      amqpUrl: harness.nodeAmqpUrl,
      siteId: harness.siteId,
      nodeId: harness.nodeId,
      topologyMode: 'passive',
      runtimeConsumerVersion: '0.1.0',
      runtimeProducerVersion: '0.1.0',
      connectionName: `amqp-mcp-edge-poison-${queueType}-${harness.runId}`,
      ...(describes ? { describeHandlerError: (error: unknown) => `${(error as Error).message} `.repeat(100) } : {}),
    });
    harness.cleanup.defer(() => edgeChannel.close());

    const deliveries: Array<{ jobId: string; deliveryAttempt?: number; redelivered?: boolean }> = [];
    edgeChannel.consumer.onJob(async (job, delivery) => {
      deliveries.push({ jobId: job.payload.jobId, deliveryAttempt: delivery.deliveryAttempt, redelivered: delivery.redelivered });

      if (job.payload.jobId.startsWith('poison-')) {
        throw new TypeError('handler bug: cannot reach amqp://node:s3cret@broker.internal');
      }

      await edgeChannel.publisher.publishResult(
        createResultMessage({
          messageId: `result-${job.payload.jobId}`,
          payload: { jobId: job.payload.jobId, attemptId: job.payload.attemptId, siteId: harness.siteId, nodeId: harness.nodeId, completedAt: new Date().toISOString(), output: { ok: true } },
        }),
      );
      await edgeChannel.consumer.ack(delivery);
    });
    await edgeChannel.consumer.start();

    const events = collectDeliveries(harness.controlChannel, harness.controlQueueName, 2, 15000);
    const deadLettered = collectDeliveries(harness.controlChannel, harness.deadLetterQueueName, 1, 15000);
    const publishJob = (jobId: string) => {
      const job = createExecutionJob({
        messageId: `message-${jobId}`,
        payload: { ...createExecutionJob().payload, jobId, attemptId: `${jobId}-attempt-1`, idempotencyKey: jobId, target: { siteId: harness.siteId, nodeId: harness.nodeId } },
      });
      harness.controlChannel.publish(jobsExchange, buildJobRoutingKey(job.payload.target, job.payload.capability), Buffer.from(JSON.stringify(job), 'utf8'), {
        contentType: 'application/json',
        messageId: job.messageId,
        persistent: true,
        userId: harness.adminUser,
      });
    };

    // The poison job first, then a good one queued behind it.
    publishJob(`poison-${harness.runId}`);
    publishJob(`good-${harness.runId}`);

    const received = (await events).map((message) => {
      const envelope = validateEnvelope(JSON.parse(message.content.toString('utf8')));
      assert.ok(envelope.messageType === 'execution.failure' || envelope.messageType === 'execution.result', envelope.messageType);
      return envelope;
    });
    const failure = received.find((envelope) => envelope.messageType === 'execution.failure');
    const result = received.find((envelope) => envelope.messageType === 'execution.result');

    assert.ok(failure?.messageType === 'execution.failure');
    assert.equal(failure.payload.jobId, `poison-${harness.runId}`);
    assert.equal(failure.payload.code, edgeFailureCodes.deliveryAttemptsExhausted);
    assert.equal(failure.payload.retryable, false);
    assert.equal(failure.payload.nodeId, harness.nodeId);
    const handlerError = failure.payload.details?.handlerError as { code?: string; name?: string; description?: string } | undefined;
    assert.equal(handlerError?.code, 'HANDLER_ERROR');
    assert.equal(handlerError?.name, 'TypeError');
    if (describes) {
      // The opted-in description, cut to 1000 characters.
      assert.equal(handlerError?.description?.length, 1000);
      assert.ok(handlerError?.description?.startsWith('handler bug: cannot reach'));
    } else {
      assert.equal(handlerError?.description, undefined);
      assert.ok(!JSON.stringify(failure).includes('s3cret'), 'the default failure event carries the handler error text');
    }
    assert.ok(result?.messageType === 'execution.result');
    assert.equal(result.payload.jobId, `good-${harness.runId}`);

    // The poison job reached the dead-letter queue the platform bound, rejected by the node queue.
    const [deadLetter] = await deadLettered;
    assert.equal(deadLetter?.properties.messageId, `message-poison-${harness.runId}`);
    const death = (deadLetter?.properties.headers?.['x-death'] as Array<{ queue?: string; reason?: string }> | undefined)?.[0];
    assert.equal(death?.queue, harness.queueName);
    assert.equal(death?.reason, 'rejected');

    // Exactly maxDeliveryAttempts (2) deliveries of the poison job, and one of the good job.
    await sleep(500);
    const poison = deliveries.filter((delivery) => delivery.jobId.startsWith('poison-'));
    assert.deepEqual(poison.map((delivery) => [delivery.deliveryAttempt, delivery.redelivered]), [[1, false], [2, true]]);
    assert.equal(deliveries.filter((delivery) => delivery.jobId.startsWith('good-')).length, 1);
    const health = await edgeChannel.consumer.health();
    assert.equal(health.details?.pendingDeliveries, 0);
    assert.equal(health.details?.failureReportErrors, undefined);
  });
}

test('publisher confirms: an event the broker nacks rejects the publish, and the job is never acked', { timeout: 60000 }, async (t) => {
  const api = await managementApiOrSkip(t);
  if (!api) {
    return;
  }

  const harness = await createLeastPrivilegeHarness(t, api);
  const { jobsExchange, eventsExchange } = defaultExchangeConfig;

  // The platform binds a queue that refuses every result: x-overflow reject-publish makes the
  // broker nack a publish it cannot enqueue, which is what a confirm channel reports.
  const fullQueue = `mcp.edge.full.${harness.siteId}.${harness.nodeId}`;
  await harness.controlChannel.assertQueue(fullQueue, { durable: true, arguments: { 'x-max-length': 0, 'x-overflow': 'reject-publish' } });
  harness.cleanup.defer(() => withChannel(harness.controlConnection, (channel) => channel.deleteQueue(fullQueue)));
  await harness.controlChannel.bindQueue(fullQueue, eventsExchange, `edge.event.result.${harness.siteId}.${harness.nodeId}`);

  const edgeChannel = await createEdgeChannel({
    amqpUrl: harness.nodeAmqpUrl,
    siteId: harness.siteId,
    nodeId: harness.nodeId,
    topologyMode: 'passive',
    runtimeConsumerVersion: '0.1.0',
    runtimeProducerVersion: '0.1.0',
    connectionName: `amqp-mcp-edge-confirms-${harness.runId}`,
  });
  harness.cleanup.defer(() => edgeChannel.close());

  // A plain publish the broker accepts resolves; one it nacks rejects.
  await edgeChannel.publisher.publishAck(
    createAckMessage({ messageId: `ack-${harness.runId}`, payload: { ...createAckMessage().payload, siteId: harness.siteId, nodeId: harness.nodeId } }),
  );
  await assert.rejects(
    () => edgeChannel.publisher.publishResult(
      createResultMessage({ messageId: `direct-result-${harness.runId}`, payload: { ...createResultMessage().payload, siteId: harness.siteId, nodeId: harness.nodeId } }),
    ),
    (error: unknown) => error instanceof EdgePublishError && error.reason === 'nacked' && error.messageId === `direct-result-${harness.runId}`,
  );

  // A worker that acks only after its result is confirmed never acks this job.
  const publishErrors: unknown[] = [];
  let acked = 0;
  edgeChannel.consumer.onJob(async (job, delivery) => {
    try {
      await edgeChannel.publisher.publishResult(
        createResultMessage({
          messageId: `result-${harness.runId}-${delivery.deliveryAttempt}`,
          payload: { jobId: job.payload.jobId, attemptId: job.payload.attemptId, siteId: harness.siteId, nodeId: harness.nodeId, completedAt: new Date().toISOString(), output: { ok: true } },
        }),
      );
    } catch (error) {
      publishErrors.push(error);
      throw error;
    }
    await edgeChannel.consumer.ack(delivery);
    acked += 1;
  });
  await edgeChannel.consumer.start();

  // A nack does not mean no queue got the event: the control queue accepts every result the full
  // queue refused, which is why a nacked event counts as "maybe delivered" and consumers dedupe
  // by messageId. It sees the ack, the direct result, one result per attempt, then the failure.
  const events = collectDeliveries(harness.controlChannel, harness.controlQueueName, 5, 15000);
  const deadLettered = collectDeliveries(harness.controlChannel, harness.deadLetterQueueName, 1, 15000);
  const job = createExecutionJob({
    messageId: `job-${harness.runId}`,
    payload: { ...createExecutionJob().payload, jobId: `job-${harness.runId}`, attemptId: `attempt-${harness.runId}`, target: { siteId: harness.siteId, nodeId: harness.nodeId } },
  });
  harness.controlChannel.publish(jobsExchange, buildJobRoutingKey(job.payload.target, job.payload.capability), Buffer.from(JSON.stringify(job), 'utf8'), {
    contentType: 'application/json',
    messageId: job.messageId,
    userId: harness.adminUser,
  });

  const received = (await events).map((message) => validateEnvelope(JSON.parse(message.content.toString('utf8'))));
  assert.deepEqual(received.map((envelope) => [envelope.messageType, envelope.messageId]), [
    ['execution.ack', `ack-${harness.runId}`],
    ['execution.result', `direct-result-${harness.runId}`],
    ['execution.result', `result-${harness.runId}-1`],
    ['execution.result', `result-${harness.runId}-2`],
    ['execution.failure', received[4]?.messageId],
  ]);
  const failure = received[4];
  assert.ok(failure?.messageType === 'execution.failure');
  assert.equal(failure.payload.code, edgeFailureCodes.deliveryAttemptsExhausted);
  assert.deepEqual(failure.payload.details?.handlerError, { code: 'HANDLER_ERROR', name: 'EdgePublishError' });

  const [deadLetter] = await deadLettered;
  assert.equal(deadLetter?.properties.messageId, job.messageId);
  assert.equal(acked, 0);
  assert.equal(publishErrors.length, 2);
  assert.ok(publishErrors.every((error) => error instanceof EdgePublishError && error.reason === 'nacked'));
  // Nothing was enqueued on the refusing queue.
  const { messageCount } = await withChannel(harness.controlConnection, (channel) => channel.checkQueue(fullQueue));
  assert.equal(messageCount, 0);
});

// --- Reconnects and channel closes ----------------------------------------------------------------

type ExecutionJob = ReturnType<typeof createExecutionJob>;

// The node's connections, as the broker lists them.
function listNodeConnections(api: ManagementApi, harness: LeastPrivilegeHarness, connectionName: string) {
  return managementGet<Array<{ name: string; client_properties?: { connection_name?: string } }>>(
    api,
    `/api/vhosts/${encodeURIComponent(harness.vhost)}/connections`,
  ).then((connections) => connections.filter((connection) => connection.client_properties?.connection_name === connectionName));
}

function publishJob(harness: LeastPrivilegeHarness, name: string): ExecutionJob {
  const job = createExecutionJob({
    messageId: `${name}-${harness.runId}`,
    payload: {
      ...createExecutionJob().payload,
      jobId: `${name}-${harness.runId}`,
      attemptId: `attempt-${name}-${harness.runId}`,
      target: { siteId: harness.siteId, nodeId: harness.nodeId },
    },
  });
  harness.controlChannel.publish(
    defaultExchangeConfig.jobsExchange,
    buildJobRoutingKey(job.payload.target, job.payload.capability),
    Buffer.from(JSON.stringify(job), 'utf8'),
    { contentType: 'application/json', messageId: job.messageId, userId: harness.adminUser },
  );
  return job;
}

// A worker whose handler holds every job until the test releases it, then acks it, and records how
// that ack went.
function holdingWorker(consumer: Pick<EdgeJobConsumer, 'onJob' | 'ack'>) {
  const held: Array<{ jobId: string; delivery: JobDeliveryContext; release: () => void }> = [];
  const outcomes: string[] = [];

  consumer.onJob(async (job, delivery) => {
    let release = () => undefined as void;
    const released = new Promise<void>((resolve) => {
      release = resolve;
    });
    held.push({ jobId: job.payload.jobId, delivery, release });
    await released;

    try {
      await consumer.ack(delivery);
      outcomes.push(`acked ${job.payload.jobId}`);
    } catch (error) {
      outcomes.push(error instanceof EdgeStaleDeliveryError ? `stale ${job.payload.jobId}` : `failed ${job.payload.jobId}: ${String(error)}`);
    }
  });

  const heldCount = (count: number) => waitFor(`${count} held deliveries`, 15000, async () => (held.length >= count ? held : undefined));
  const outcomeCount = (count: number) => waitFor(`${count} outcomes`, 15000, async () => (outcomes.length >= count ? outcomes : undefined));
  return { held, outcomes, heldCount, outcomeCount };
}

// The node's queue holds nothing, acked or not: closing every node connection returns an unacked
// job to the ready count.
async function assertNodeQueueEmpty(harness: LeastPrivilegeHarness): Promise<void> {
  const { messageCount } = await withChannel(harness.controlConnection, (channel) => channel.checkQueue(harness.queueName));
  assert.equal(messageCount, 0);
}

function nodeChannel(harness: LeastPrivilegeHarness, connectionName: string) {
  const config = {
    amqpUrl: harness.nodeAmqpUrl,
    siteId: harness.siteId,
    nodeId: harness.nodeId,
    topologyMode: 'passive' as const,
    runtimeConsumerVersion: '0.1.0',
    runtimeProducerVersion: '0.1.0',
    reconnectDelayMs: 100,
    connectionName,
  };
  const connectionManager = new EdgeChannelConnectionManager(config);
  const publisher = new EdgeEventPublisher(config, connectionManager);
  const consumer = new EdgeJobConsumer(config, connectionManager, { failurePublisher: publisher });
  return { connectionManager, publisher, consumer };
}

test('reconnect: a job in flight when the connection drops is redelivered, and its old delivery cannot be acked', { timeout: 60000 }, async (t) => {
  const api = await managementApiOrSkip(t);
  if (!api) {
    return;
  }

  const harness = await createLeastPrivilegeHarness(t, api);
  const connectionName = `amqp-mcp-edge-inflight-${harness.runId}`;
  const { connectionManager, consumer, publisher } = nodeChannel(harness, connectionName);
  harness.cleanup.defer(() => connectionManager.close());
  const worker = holdingWorker(consumer);
  await consumer.start();
  const ack = (name: string) =>
    createAckMessage({ messageId: `${name}-${harness.runId}`, payload: { ...createAckMessage().payload, siteId: harness.siteId, nodeId: harness.nodeId } });
  await publisher.publishAck(ack('before'));
  // Each component's health reports the other too.
  assert.equal((await consumer.health()).publisherActive, true);
  assert.equal((await publisher.health()).consumerActive, true);

  const [original] = await waitFor('the node connection to be listed', 15000, async () => {
    const connections = await listNodeConnections(api, harness, connectionName);
    return connections.length === 1 ? connections : undefined;
  });

  publishJob(harness, 'job-1');
  const [first] = await worker.heldCount(1);
  assert.equal(first!.delivery.redelivered, false);

  // The broker drops the connection with job 1 unacked, so it delivers job 1 again on the new
  // connection, where delivery tags start over: the redelivery has the same tag.
  await managementRequest(api, 'DELETE', `/api/connections/${encodeURIComponent(original!.name)}`);
  const [, second] = await worker.heldCount(2);
  assert.equal(second!.jobId, first!.jobId);
  assert.equal(second!.delivery.redelivered, true);
  assert.equal(second!.delivery.deliveryAttempt, 2);
  assert.equal(second!.delivery.deliveryTag, first!.delivery.deliveryTag);

  // The consumer has subscribed on the new connection. The publisher has not published on it yet,
  // so it is not reported active until it does.
  let health = await publisher.health();
  assert.equal(health.connected, true);
  assert.equal(health.consumerActive, true);
  assert.equal(health.publisherActive, false);
  await publisher.publishAck(ack('after'));
  health = await publisher.health();
  assert.equal(health.publisherActive, true);

  // The first handler finishes: its ack is refused, without reaching the broker. Sent on the new
  // channel it would have acked the redelivery (same tag) under the second handler's feet.
  first!.release();
  assert.deepEqual(await worker.outcomeCount(1), [`stale ${first!.jobId}`]);
  second!.release();
  assert.deepEqual(await worker.outcomeCount(2), [`stale ${first!.jobId}`, `acked ${first!.jobId}`]);

  // The new consumer channel is still open: the next job runs on it.
  publishJob(harness, 'job-2');
  const [, , third] = await worker.heldCount(3);
  third!.release();
  assert.deepEqual((await worker.outcomeCount(3)).at(-1), `acked ${third!.jobId}`);

  const consumerHealth = await consumer.health();
  assert.equal(consumerHealth.connected, true);
  assert.equal(consumerHealth.consumerActive, true);
  assert.equal(consumerHealth.details?.discardedDeliveries, 1);
  assert.equal(consumerHealth.details?.pendingDeliveries, 0);
  const connectionHealth = await connectionManager.health();
  assert.equal(connectionHealth.details?.consumerChannelReopens, 0);
  assert.equal(connectionHealth.details?.lastChannelError, undefined);
  // One replacement connection; the management API lists it a little later.
  await waitFor('the replacement connection to be listed', 15000, async () => {
    const connections = await listNodeConnections(api, harness, connectionName);
    return connections.length === 1 && connections[0]?.name !== original!.name ? connections : undefined;
  });

  await connectionManager.close();
  await assertNodeQueueEmpty(harness);
});

test('channel close: a consumer channel the broker closes is reopened on the same connection, and the job is redelivered', { timeout: 60000 }, async (t) => {
  const api = await managementApiOrSkip(t);
  if (!api) {
    return;
  }

  const harness = await createLeastPrivilegeHarness(t, api);
  const connectionName = `amqp-mcp-edge-channel-${harness.runId}`;
  const { connectionManager, consumer } = nodeChannel(harness, connectionName);
  harness.cleanup.defer(() => connectionManager.close());
  const worker = holdingWorker(consumer);
  await consumer.start();

  const [original] = await waitFor('the node connection to be listed', 15000, async () => {
    const connections = await listNodeConnections(api, harness, connectionName);
    return connections.length === 1 ? connections : undefined;
  });

  publishJob(harness, 'job-1');
  const [first] = await worker.heldCount(1);

  // An ack of a delivery tag the broker never sent makes it close the channel with
  // PRECONDITION_FAILED, as an ack of a tag from an earlier channel did before this fix.
  const brokerClose = async () => {
    const channel = await connectionManager.getConsumerChannel();
    channel.ack({ fields: { deliveryTag: 999 } } as ConsumeMessage);
  };
  await brokerClose();

  const [, second] = await worker.heldCount(2);
  assert.equal(second!.jobId, first!.jobId);
  assert.equal(second!.delivery.redelivered, true);
  let connectionHealth = await connectionManager.health();
  assert.equal(connectionHealth.connected, true);
  assert.equal(connectionHealth.details?.consumerChannelReopens, 1);
  assert.match(String(connectionHealth.details?.lastChannelError), /PRECONDITION_FAILED - unknown delivery tag 999/);

  first!.release();
  assert.deepEqual(await worker.outcomeCount(1), [`stale ${first!.jobId}`]);

  // A second close soon after the first is reopened too, after the reconnect delay.
  await brokerClose();
  const [, , third] = await worker.heldCount(3);
  assert.equal(third!.jobId, first!.jobId);
  second!.release();
  third!.release();
  assert.deepEqual((await worker.outcomeCount(3)).slice(1).sort(), [`acked ${first!.jobId}`, `stale ${first!.jobId}`].sort());

  publishJob(harness, 'job-2');
  const [, , , fourth] = await worker.heldCount(4);
  fourth!.release();
  assert.equal((await worker.outcomeCount(4)).at(-1), `acked ${fourth!.jobId}`);

  // All of it on the connection the node opened first.
  const connections = await listNodeConnections(api, harness, connectionName);
  assert.deepEqual(connections.map((connection) => connection.name), [original!.name]);
  connectionHealth = await connectionManager.health();
  assert.equal(connectionHealth.details?.consumerChannelReopens, 2);
  assert.equal(connectionHealth.details?.reconnectAttempts, 0);
  assert.equal(connectionHealth.consumerActive, true);
  // The consumer's own snapshot carries the connection's details too.
  const consumerHealth = await consumer.health();
  assert.equal(consumerHealth.details?.discardedDeliveries, 2);
  assert.equal(consumerHealth.details?.consumerChannelReopens, 2);
  assert.match(String(consumerHealth.details?.lastChannelError), /PRECONDITION_FAILED/);

  await connectionManager.close();
  await assertNodeQueueEmpty(harness);
});

test('channel close: a publish to a deleted events exchange closes only the publisher channel, which is reopened and checks the exchange again', { timeout: 60000 }, async (t) => {
  const api = await managementApiOrSkip(t);
  if (!api) {
    return;
  }

  const harness = await createLeastPrivilegeHarness(t, api);
  const { eventsExchange } = defaultExchangeConfig;
  const connectionName = `amqp-mcp-edge-publisher-${harness.runId}`;
  const { connectionManager, publisher } = nodeChannel(harness, connectionName);
  harness.cleanup.defer(() => connectionManager.close());
  const ack = (name: string) =>
    createAckMessage({ messageId: `${name}-${harness.runId}`, payload: { ...createAckMessage().payload, siteId: harness.siteId, nodeId: harness.nodeId } });

  const received = collectDeliveries(harness.controlChannel, harness.controlQueueName, 2, 20000);
  await publisher.publishAck(ack('before'));
  assert.equal((await publisher.health()).publisherActive, true);
  const [original] = await waitFor('the node connection to be listed', 15000, async () => {
    const connections = await listNodeConnections(api, harness, connectionName);
    return connections.length === 1 ? connections : undefined;
  });

  // The exchange disappears. The broker answers the next publish by closing the channel (404).
  await harness.controlChannel.deleteExchange(eventsExchange);
  await assert.rejects(
    () => publisher.publishAck(ack('lost')),
    (error: unknown) => error instanceof EdgePublishError && error.reason === 'channel-closed',
  );
  assert.equal((await publisher.health()).publisherActive, false);

  const reopened = await waitFor('the publisher channel to be reopened', 15000, async () => {
    const health = await connectionManager.health();
    return health.details?.publisherChannelReopens === 1 ? health : undefined;
  });
  assert.equal(reopened.connected, true);
  assert.match(String(reopened.details?.lastChannelError), /NOT_FOUND - no exchange 'mcp\.edge\.events'/);

  // The reopened channel checks the exchange before it publishes, so the node learns why.
  await assert.rejects(
    () => publisher.publishAck(ack('missing')),
    (error: unknown) => error instanceof EdgeTopologyError && error.kind === 'exchange' && error.entityName === eventsExchange,
  );

  // Once the control plane restores the exchange, publishing works again, on the same connection.
  await harness.controlChannel.assertExchange(eventsExchange, 'topic', { durable: true });
  await harness.controlChannel.bindQueue(harness.controlQueueName, eventsExchange, `edge.event.*.${harness.siteId}.${harness.nodeId}`);
  await publisher.publishAck(ack('after'));
  const publisherHealth = await publisher.health();
  assert.equal(publisherHealth.publisherActive, true);
  assert.equal(publisherHealth.details?.publisherChannelReopens, 1);
  assert.equal(publisherHealth.details?.consumerChannelReopens, 0);

  const messages = await received;
  assert.deepEqual(messages.map((message) => message.properties.messageId), [`before-${harness.runId}`, `after-${harness.runId}`]);
  const connections = await listNodeConnections(api, harness, connectionName);
  assert.deepEqual(connections.map((connection) => connection.name), [original!.name]);
  assert.equal((await connectionManager.health()).details?.reconnectAttempts, 0);
});

test('broker cancel: a passive consumer whose queue is deleted while it is connected stops and reports it', { timeout: 60000 }, async (t) => {
  const api = await managementApiOrSkip(t);
  if (!api) {
    return;
  }

  const harness = await createLeastPrivilegeHarness(t, api);
  const connectionName = `amqp-mcp-edge-cancel-${harness.runId}`;
  const { connectionManager, consumer } = nodeChannel(harness, connectionName);
  harness.cleanup.defer(() => connectionManager.close());
  await consumer.start();
  assert.equal((await consumer.health()).consumerActive, true);

  await harness.controlChannel.deleteQueue(harness.queueName);

  // The broker cancels the subscription; the consumer checks its queue again, finds it gone, stops.
  const stopped = await waitFor('the consumer to stop', 15000, async () => {
    const health = await consumer.health();
    return health.consumerActive === false && health.details?.topologyError !== undefined ? health : undefined;
  });
  assert.equal(stopped.connected, true);
  assert.equal(stopped.details?.brokerCancellations, 1);
  assert.match(String(stopped.details?.topologyError), new RegExp(`queue '${escapeRegExp(harness.queueName)}' does not exist`));
  const connectionHealth = await connectionManager.health();
  assert.equal(connectionHealth.details?.consumerChannelReopens, 0);
  assert.equal(connectionHealth.details?.reconnectAttempts, 0);
});
