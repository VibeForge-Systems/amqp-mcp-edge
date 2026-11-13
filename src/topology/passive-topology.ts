export type TopologyEntityKind = 'exchange' | 'queue';

export interface TopologyEntity {
  kind: TopologyEntityKind;
  name: string;
}

/** The part of an amqplib channel a passive topology check uses. */
export interface PassiveCheckChannel {
  checkExchange(exchange: string): Promise<unknown>;
  checkQueue(queue: string): Promise<unknown>;
  close(): Promise<void>;
}

const NOT_FOUND = 404;
const ACCESS_REFUSED = 403;

const passiveModeHint =
  "topologyMode 'passive' never declares topology: the control plane or operator must declare it " +
  '(see "Broker topology" in docs/PROTOCOL.md)';

/**
 * A passive topology check failed: the exchange or queue is missing, or the broker refused to let
 * this node's user check it. Thrown by startup (`createEdgeChannel`, `consumer.start()`) and by
 * `publisher.verifyTopology()` in passive mode.
 */
export class EdgeTopologyError extends Error {
  public readonly kind: TopologyEntityKind;

  public readonly entityName: string;

  /** The AMQP reply code the broker closed the check channel with, such as 404 or 403. */
  public readonly replyCode?: number;

  public constructor(
    message: string,
    details: { kind: TopologyEntityKind; entityName: string; replyCode?: number; cause?: unknown },
  ) {
    super(message, { cause: details.cause });
    this.name = 'EdgeTopologyError';
    this.kind = details.kind;
    this.entityName = details.entityName;
    this.replyCode = details.replyCode;
  }
}

function replyCodeOf(error: unknown): number | undefined {
  const code = (error as { code?: unknown } | null)?.code;
  return typeof code === 'number' ? code : undefined;
}

function toTopologyError(entity: TopologyEntity, error: unknown): EdgeTopologyError | undefined {
  const replyCode = replyCodeOf(error);

  // Without an AMQP reply code this is not the broker's verdict on the entity (the connection
  // dropped, for example); let the caller see the original error.
  if (replyCode === undefined) {
    return undefined;
  }

  const subject = `${entity.kind} '${entity.name}'`;
  let message: string;

  if (replyCode === NOT_FOUND) {
    message = `passive topology check failed: ${subject} does not exist (NOT_FOUND). ${passiveModeHint}`;
  } else if (replyCode === ACCESS_REFUSED) {
    message =
      `passive topology check failed: the broker refused to let this user check ${subject} (ACCESS_REFUSED). ` +
      'RabbitMQ 4 answers a passive declare only for a user with at least one of the configure, write ' +
      'or read permissions on the name; check the node\'s permissions and that the name matches the ' +
      'topology the control plane declared';
  } else {
    const brokerMessage = error instanceof Error ? error.message : String(error);
    message = `passive topology check failed for ${subject} (reply code ${replyCode}): ${brokerMessage}`;
  }

  return new EdgeTopologyError(message, { kind: entity.kind, entityName: entity.name, replyCode, cause: error });
}

/**
 * Checks, with passive declares only, that each entity exists. Never declares, binds or deletes.
 *
 * Takes a dedicated channel and always closes it: a failed check makes the broker close the
 * channel it ran on, so it must not be a channel the consumer or publisher depends on.
 */
export async function checkTopology(channel: PassiveCheckChannel, entities: readonly TopologyEntity[]): Promise<void> {
  try {
    for (const entity of entities) {
      try {
        if (entity.kind === 'exchange') {
          await channel.checkExchange(entity.name);
        } else {
          await channel.checkQueue(entity.name);
        }
      } catch (error) {
        throw toTopologyError(entity, error) ?? error;
      }
    }
  } finally {
    await channel.close().catch(() => undefined);
  }
}
