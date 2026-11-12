import type { EdgeChannelConfig } from '../config/index.js';
import { resolveConfig } from '../config/index.js';
import { EdgeJobConsumer } from '../consumer/index.js';
import { EdgeEventPublisher } from '../publisher/index.js';

import { EdgeChannelConnectionManager } from './connection-manager.js';

type EdgeJobConsumerSurface = Pick<
  EdgeJobConsumer,
  'start' | 'stop' | 'onJob' | 'ack' | 'nack' | 'health'
>;

type EdgeEventPublisherSurface = Pick<
  EdgeEventPublisher,
  'publishAck' | 'publishProgress' | 'publishResult' | 'publishFailure' | 'health'
>;

export interface EdgeChannel {
  consumer: EdgeJobConsumerSurface;
  publisher: EdgeEventPublisherSurface;
  close(): Promise<void>;
}

/**
 * Connects to the broker and returns a consumer and publisher that share the connection.
 *
 * With `topologyMode: 'passive'` it checks that the consumer queue and the events exchange exist
 * before returning, and rejects with an `EdgeTopologyError` naming the first one that is missing.
 * It then closes the connection, so a failed startup leaves no reconnect loop running.
 */
export async function createEdgeChannel(config: EdgeChannelConfig): Promise<EdgeChannel> {
  const connectionManager = new EdgeChannelConnectionManager(config);
  // Constructing these validates the config, before a connection is opened.
  const consumer = new EdgeJobConsumer(config, connectionManager);
  const publisher = new EdgeEventPublisher(config, connectionManager);

  await connectionManager.connect();

  if (resolveConfig(config).topologyMode === 'passive') {
    try {
      await consumer.verifyTopology();
      await publisher.verifyTopology();
    } catch (error) {
      await connectionManager.close();
      throw error;
    }
  }

  return {
    consumer,
    publisher,
    async close(): Promise<void> {
      await consumer.stop().catch(() => undefined);
      await connectionManager.close();
    },
  };
}

export { EdgeChannelConnectionManager } from './connection-manager.js';
export { EdgeTopologyError } from '../topology/index.js';