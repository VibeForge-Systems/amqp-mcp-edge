# Security policy

## Supported versions

`@vibeforge-systems/amqp-mcp-edge` is pre-1.0. Security fixes land on `main` and ship in the next
0.x release; earlier releases are not patched.

## Reporting a vulnerability

Report security problems **privately**. Do not open a public issue.

- **Preferred:** a private report through GitHub security advisories, at
  <https://github.com/VibeForge-Systems/amqp-mcp-edge/security/advisories/new>.
- **Otherwise:** contact the maintainers through the security contact on
  <https://www.vibeforgesystems.com/>.

Include what the problem is and what an attacker gains, the affected version or commit, steps to
reproduce (ideally a minimal proof of concept), and a suggested fix if you have one.

We aim to acknowledge a report within **3 business days**, and to give an initial assessment and
remediation plan within **10 business days**. We agree a disclosure date with you, and credit you
in the release notes unless you prefer otherwise.

## Security model

The library moves execution jobs and their lifecycle events between a control plane and edge
workers. Who may publish and consume them is decided by the broker.

- **The broker is the access control.** The library has no authentication or authorization of its
  own. Give each edge worker its own RabbitMQ user, limited by permissions to its own queue and
  to publishing on the events exchange, with no configure permission, and run it with
  `topologyMode: 'passive'`. [docs/PROTOCOL.md](docs/PROTOCOL.md#least-privilege-permissions)
  gives the permission set and the topology the control plane declares.
- **`user_id` is the node identity.** Every event carries the broker-validated AMQP `user_id`
  property. Consumers must reject events without it; the broker checks it only when it is set.
- **Use TLS off-host.** Use `amqps://` for any broker that is not on the same host or a trusted
  private network; credentials in the URL are otherwise sent in clear text.
- **Envelopes are validated, payloads are not interpreted.** Inbound jobs are checked for shape
  and compatibility before your handler runs. `payload.input` is passed to your handler as-is:
  treat it as untrusted input.
- **Size limits.** `maxMessageSizeBytes` bounds the messages the consumer accepts; keep it as small
  as your jobs allow.
