import { createHash } from 'node:crypto';

/**
 * Serializes a JSON value in the RFC 8785 (JCS) canonical form: no whitespace, object members
 * sorted by the UTF-16 code units of their names, and strings and numbers written as ECMAScript's
 * `JSON.stringify` writes them. Members whose value is `undefined` are left out, as
 * `JSON.stringify` leaves them out.
 *
 * Throws a `TypeError` for anything JSON cannot carry: a non-finite number, a bigint, a function, a
 * symbol, or `undefined` in an array.
 */
export function canonicalJson(value: unknown): string {
  if (value === null) {
    return 'null';
  }

  switch (typeof value) {
    case 'string':
      return JSON.stringify(value);
    case 'boolean':
      return value ? 'true' : 'false';
    case 'number':
      if (!Number.isFinite(value)) {
        throw new TypeError(`canonical JSON cannot carry the number ${String(value)}`);
      }

      return JSON.stringify(value);
    case 'object': {
      if (Array.isArray(value)) {
        return `[${value
          .map((item, index) => {
            if (item === undefined) {
              throw new TypeError(`canonical JSON cannot carry undefined (array index ${index})`);
            }

            return canonicalJson(item);
          })
          .join(',')}]`;
      }

      const record = value as Record<string, unknown>;
      const members = Object.keys(record)
        .filter((key) => record[key] !== undefined)
        .sort()
        .map((key) => `${JSON.stringify(key)}:${canonicalJson(record[key])}`);
      return `{${members.join(',')}}`;
    }
    default:
      throw new TypeError(`canonical JSON cannot carry a value of type ${typeof value}`);
  }
}

/** `sha256:` followed by the lowercase hex SHA-256 of the UTF-8 bytes of `canonicalJson(value)`. */
export function canonicalJsonSha256(value: unknown): string {
  return `sha256:${createHash('sha256').update(canonicalJson(value), 'utf8').digest('hex')}`;
}
