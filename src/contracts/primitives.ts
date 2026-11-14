import { z } from 'zod';

/** An RFC 3339 date-time with seconds and a `Z` or `±hh:mm` offset; see "Timestamp Format". */
export const timestampSchema = z.string().datetime({ offset: true });

/**
 * A package's dist hash: an algorithm tag, a colon, and 64 or 128 lowercase hex digits, with
 * nothing before or after. The package format defines the tag and how the value is computed (for
 * example `mcpod-dist-v2:<64 hex>`); this protocol carries it and compares it as an exact string.
 */
export const distHashPattern = /^[a-z0-9][a-z0-9.-]*:(?:[0-9a-f]{64}|[0-9a-f]{128})$/;

export const distHashSchema = z.string().regex(distHashPattern, {
  message: 'must be an algorithm tag, a colon and 64 or 128 lowercase hex digits',
});

/** The identity of one published package release. */
export interface PackageReleaseIdentity {
  packageId: string;
  packageVersion: string;
  releaseId: string;
}

export const packageReleaseIdentitySchema = z
  .object({
    packageId: z.string().min(1),
    packageVersion: z.string().min(1),
    releaseId: z.string().min(1),
  })
  .passthrough();
