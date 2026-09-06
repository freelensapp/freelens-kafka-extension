import { createHash } from "node:crypto";

export interface AuthorizationPins {
  contextSha256: string;
  targetHostSha256: string;
}

export interface ReadOnlyTargetAuthorization {
  context: string;
  targetHost: string;
}

function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function requiredSha256(value: string | undefined, label: string): string {
  const normalized = value?.trim().toLowerCase();
  if (!normalized || !/^[0-9a-f]{64}$/.test(normalized)) {
    throw new Error(`Read-only target authorization requires ${label}`);
  }
  return normalized;
}

export function readAuthorizationPins(environment: NodeJS.ProcessEnv): AuthorizationPins {
  return {
    contextSha256: requiredSha256(environment.AUTHORIZED_KUBE_CONTEXT_SHA256, "AUTHORIZED_KUBE_CONTEXT_SHA256"),
    targetHostSha256: requiredSha256(
      environment.AUTHORIZED_KAFKA_TARGET_HOST_SHA256,
      "AUTHORIZED_KAFKA_TARGET_HOST_SHA256",
    ),
  };
}

export function authorizeReadOnlyTarget(
  environment: NodeJS.ProcessEnv,
  pins: AuthorizationPins = readAuthorizationPins(environment),
): ReadOnlyTargetAuthorization {
  const context = environment.KUBE_CONTEXT;
  const authorizedContext = environment.AUTHORIZED_KUBE_CONTEXT;
  const targetHost = environment.KAFKA_TARGET_HOST?.trim().toLowerCase();
  const authorizedTargetHost = environment.AUTHORIZED_KAFKA_TARGET_HOST?.trim().toLowerCase();
  if (
    environment.ALLOW_REAL_READ_ONLY !== "1" ||
    !context ||
    context !== authorizedContext ||
    sha256(context) !== pins.contextSha256 ||
    !targetHost ||
    targetHost !== authorizedTargetHost ||
    sha256(targetHost) !== pins.targetHostSha256
  ) {
    throw new Error("Read-only target authorization refused");
  }
  return { context, targetHost };
}

export function selectSingleAuthorizedTarget<T>(
  candidates: T[],
  targetHost: string,
  matchesHost: (candidate: T, targetHost: string) => boolean,
): T {
  const matches = candidates.filter((candidate) => matchesHost(candidate, targetHost));
  if (matches.length !== 1) throw new Error("Authorized target selection was not unique");
  return matches[0];
}
