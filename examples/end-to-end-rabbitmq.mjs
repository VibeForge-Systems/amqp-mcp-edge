import { randomUUID } from 'node:crypto';

import { connect } from 'amqplib';

import {
  buildJobRoutingKey,
  buildRoutingKey,
  createEdgeChannel,
  validateEnvelope,
} from '../dist/index.js';

const candidateAmqpUrls = [
  process.env.AMQP_EXAMPLE_URL,
  process.env.AMQP_INTEGRATION_URL,
  'amqp://mcp:discovery@127.0.0.1:5672',
  'amqp://guest:guest@127.0.0.1:5672',
].filter((value, index, values) => Boolean(value) && values.indexOf(value) === index);
const runId = randomUUID().slice(0, 8);
const siteId = `site-${runId}`;
const nodeId = `node-${runId}`;
const jobsExchange = `mcp.edge.jobs.example.${runId}`;
const eventsExchange = `mcp.edge.events.example.${runId}`;
const deadLetterExchange = `mcp.edge.dlx.example.${runId}`;
const workerQueueName = `mcp.edge.consumer.${siteId}.${nodeId}.${runId}`;
const controlQueueName = `mcp.edge.control.${siteId}.${nodeId}.${runId}`;

function log(prefix, value) {
  process.stdout.write(`${prefix} ${JSON.stringify(value)}\n`);
}

async function connectToBroker() {
  const failures = [];

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

async function main() {
  const { connection: controlConnection, amqpUrl } = await connectToBroker();
  const controlChannel = await controlConnection.createChannel();

  log('broker connected', { amqpUrl });

  await controlChannel.assertExchange(jobsExchange, 'topic', { durable: true });
  await controlChannel.assertExchange(eventsExchange, 'topic', { durable: true });
  await controlChannel.assertExchange(deadLetterExchange, 'topic', { durable: true });
  // Exclusive: RabbitMQ 4.x refuses transient non-exclusive queues.
  await controlChannel.assertQueue(controlQueueName, { durable: false, exclusive: true });
  await controlChannel.bindQueue(controlQueueName, eventsExchange, `edge.event.*.${siteId}.${nodeId}`);

  const edgeChannel = await createEdgeChannel({
    amqpUrl,
    siteId,
    nodeId,
    jobsExchange,
    eventsExchange,
    deadLetterExchange,
    consumerQueueName: workerQueueName,
    consumerQueueOptions: {
      durable: false,
      exclusive: true,
    },
    connectionName: `amqp-mcp-edge-example-${runId}`,
  });

  edgeChannel.consumer.onJob(async (job, delivery) => {
    log('worker received job', {
      jobId: job.payload.jobId,
      routingKey: delivery.routingKey,
      capability: job.payload.capability,
    });

    const ackEnvelope = {
      specVersion: '1.0',
      messageType: 'execution.ack',
      messageId: `ack-${runId}`,
      timestamp: new Date().toISOString(),
      producer: {
        service: 'example-edge-worker',
        version: '0.1.0',
      },
      compatibility: {
        channelVersion: '0.1.0',
        packageVersion: '0.1.0',
      },
      payload: {
        jobId: job.payload.jobId,
        attemptId: job.payload.attemptId,
        siteId,
        nodeId,
        accepted: true,
      },
    };

    const progressEnvelope = {
      specVersion: '1.0',
      messageType: 'execution.progress',
      messageId: `progress-${runId}`,
      timestamp: new Date().toISOString(),
      producer: {
        service: 'example-edge-worker',
        version: '0.1.0',
      },
      compatibility: {
        channelVersion: '0.1.0',
        packageVersion: '0.1.0',
      },
      payload: {
        jobId: job.payload.jobId,
        attemptId: job.payload.attemptId,
        siteId,
        nodeId,
        state: 'running',
        percent: 100,
        metrics: {
          documentsProcessed: 3,
        },
        message: 'Synthetic search completed',
      },
    };

    const resultEnvelope = {
      specVersion: '1.0',
      messageType: 'execution.result',
      messageId: `result-${runId}`,
      timestamp: new Date().toISOString(),
      producer: {
        service: 'example-edge-worker',
        version: '0.1.0',
      },
      compatibility: {
        channelVersion: '0.1.0',
        packageVersion: '0.1.0',
      },
      payload: {
        jobId: job.payload.jobId,
        attemptId: job.payload.attemptId,
        siteId,
        nodeId,
        completedAt: new Date().toISOString(),
        output: {
          documents: [
            { id: 'doc-1', title: 'Edge Eventing Overview' },
            { id: 'doc-2', title: 'MCP Worker Deployment Notes' },
            { id: 'doc-3', title: 'AMQP Routing Reference' },
          ],
        },
      },
    };

    await edgeChannel.publisher.publishAck(ackEnvelope);
    await edgeChannel.publisher.publishProgress(progressEnvelope);
    await edgeChannel.publisher.publishResult(resultEnvelope);
    await edgeChannel.consumer.ack(delivery);
  });

  await edgeChannel.consumer.start();

  const jobEnvelope = {
    specVersion: '1.0',
    messageType: 'execution.job',
    messageId: `job-${runId}`,
    timestamp: new Date().toISOString(),
    producer: {
      service: 'example-control-plane',
      version: '0.1.0',
    },
    compatibility: {
      channelVersion: '0.1.0',
      packageVersion: '0.1.0',
    },
    payload: {
      jobId: `job-${runId}`,
      attemptId: `attempt-${runId}`,
      idempotencyKey: `job-${runId}-attempt-${runId}`,
      timeoutMs: 30000,
      target: {
        siteId,
        nodeId,
      },
      createdBy: {
        controlPlane: 'example-control-plane',
      },
      capability: {
        kind: 'tool',
        name: 'search.documents',
        version: '2026-03',
      },
      input: {
        query: 'show me documents about edge workers',
      },
    },
  };

  const observedMessages = [];
  const consumeResult = await controlChannel.consume(controlQueueName, (message) => {
    if (!message) {
      return;
    }

    controlChannel.ack(message);
    const parsed = validateEnvelope(JSON.parse(message.content.toString('utf8')));
    observedMessages.push(parsed);
    log('control-plane observed event', {
      type: parsed.messageType,
      routingKey: message.fields.routingKey,
      payload: parsed.payload,
    });
  });

  const routingKey = buildJobRoutingKey(jobEnvelope.payload.target, jobEnvelope.payload.capability);
  log('control-plane publishing job', {
    routingKey,
    jobId: jobEnvelope.payload.jobId,
  });

  controlChannel.publish(jobsExchange, routingKey, Buffer.from(JSON.stringify(jobEnvelope), 'utf8'), {
    contentType: 'application/json',
    type: jobEnvelope.messageType,
    messageId: jobEnvelope.messageId,
  });

  const deadline = Date.now() + 15000;
  while (observedMessages.length < 3 && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 200));
  }

  if (observedMessages.length < 3) {
    throw new Error('timed out waiting for ack, progress, and result events');
  }

  log('simulation complete', {
    observedTypes: observedMessages.map((message) => message.messageType),
    eventRoutingKeys: observedMessages.map((message) => buildRoutingKey(message.messageType, {
      siteId: message.payload.siteId,
      nodeId: message.payload.nodeId,
    })),
  });

  await controlChannel.cancel(consumeResult.consumerTag).catch(() => undefined);
  await edgeChannel.close();
  await controlChannel.deleteQueue(controlQueueName).catch(() => undefined);
  await controlChannel.close().catch(() => undefined);
  await controlConnection.close().catch(() => undefined);
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.stack ?? error.message : String(error)}\n`);
  process.exitCode = 1;
});