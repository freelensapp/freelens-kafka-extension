import { Sha256 } from "@aws-crypto/sha256-js";
import { defaultProvider } from "@aws-sdk/credential-provider-node";
import { formatUrl } from "@aws-sdk/util-format-url";
import { SignatureV4 } from "@smithy/signature-v4";
import { EXTENSION_VERSION } from "../../common/extension-version";

import type { SASLOptions } from "kafkajs";

/**
 * MSK IAM token for SASL/OAUTHBEARER, built the way the AWS signer for MSK builds it
 * (aws-msk-iam-sasl-signer-js 1.0.3, `generateAuthTokenFromCredentialsProvider`): a presigned
 * `kafka-cluster:Connect` request, SigV4 in the query string, base64url encoded. The composition
 * lives here instead of in that package because every ready-made MSK IAM library pulls
 * `@aws-sdk/credential-providers`, which Rolldown cannot bundle into the single-file release build
 * (SPEC-017 decision log). The signature, the credential chain and the URL formatting stay with the
 * AWS packages.
 */

const SIGNING_SERVICE = "kafka-cluster";
const ACTION = "kafka-cluster:Connect";
/** Longest life MSK accepts for a token; a shorter one when the credentials expire earlier. */
const TOKEN_TTL_SECONDS = 900;

export interface AwsCredentials {
  accessKeyId: string;
  secretAccessKey: string;
  sessionToken?: string;
  expiration?: Date;
}

export type AwsCredentialsProvider = () => Promise<AwsCredentials>;

export interface MskIamOptions {
  /** AWS region of the cluster; the signing endpoint is derived from it, never from the brokers. */
  region: string;
  /** Shared-config profile to resolve the credentials from; the SDK default chain otherwise. */
  profile?: string;
  /** Credentials provider in place of the SDK chain. Tests only. */
  credentials?: AwsCredentialsProvider;
}

export interface MskIamToken {
  token: string;
  /** Epoch milliseconds after which the token is no longer valid. */
  expiresAt: number;
}

export function mskIamHostname(region: string): string {
  return `kafka.${region}.amazonaws.com`;
}

/** The credentials chain of the AWS SDK, memoized and refreshed by the SDK itself. */
export function awsCredentialsProvider(profile?: string): AwsCredentialsProvider {
  return defaultProvider(profile ? { profile } : {});
}

export async function generateMskIamToken(options: MskIamOptions & { now?: Date }): Promise<MskIamToken> {
  const region = options.region.trim();
  if (!region) throw new Error("AWS region is required for AWS IAM (MSK) authentication");
  const provider = options.credentials ?? awsCredentialsProvider(options.profile);
  const credentials = await provider();
  if (!credentials.accessKeyId || !credentials.secretAccessKey) {
    throw new Error(
      options.profile
        ? `No AWS credentials were resolved for profile "${options.profile}"`
        : "No AWS credentials were resolved by the AWS SDK default provider chain",
    );
  }

  const now = options.now ?? new Date();
  const ttlSeconds = credentials.expiration
    ? Math.max(1, Math.min(Math.floor((credentials.expiration.getTime() - now.getTime()) / 1000), TOKEN_TTL_SECONDS))
    : TOKEN_TTL_SECONDS;
  const hostname = mskIamHostname(region);
  const signer = new SignatureV4({
    service: SIGNING_SERVICE,
    region,
    credentials,
    sha256: Sha256,
    applyChecksum: false,
  });
  const signed = await signer.presign(
    {
      method: "GET",
      protocol: "https:",
      hostname,
      path: "/",
      headers: { host: hostname },
      query: { Action: ACTION },
    },
    { expiresIn: ttlSeconds, signingDate: now },
  );
  const url = formatUrl({
    ...signed,
    query: { ...signed.query, "User-Agent": `freelens-kafka-extension/${EXTENSION_VERSION}` },
  });

  return {
    token: Buffer.from(url, "utf8").toString("base64url"),
    expiresAt: now.getTime() + ttlSeconds * 1000,
  };
}

/**
 * kafkajs SASL settings for MSK IAM: OAUTHBEARER with a token minted at every authentication, so a
 * reconnection after the 15 minutes gets a fresh one and refreshed credentials are picked up.
 */
export function createMskIamSasl(options: MskIamOptions): SASLOptions {
  const credentials = options.credentials ?? awsCredentialsProvider(options.profile);
  return {
    mechanism: "oauthbearer",
    oauthBearerProvider: async () => ({
      value: (await generateMskIamToken({ region: options.region, profile: options.profile, credentials })).token,
    }),
  };
}
