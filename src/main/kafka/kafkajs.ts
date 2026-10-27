import * as kafkajsNamespace from "kafkajs";

/**
 * The members of kafkajs that Node cannot import by name. kafkajs is CommonJS, and Node finds the named exports of a
 * CommonJS module by reading its source: it stops at the spread of kafkajs's `module.exports`, so these members are
 * missing from the module's named exports when Node itself loads the extension's code, as `tsx` does for the itests.
 * They are taken from `module.exports`, the module's default export there; the bundle and Vitest, which interoperate
 * with CommonJS on their own, get the same objects. `CompressionCodecs` must be kafkajs's own object: registering a
 * codec mutates it.
 */
const kafkajs: typeof kafkajsNamespace =
  (kafkajsNamespace as typeof kafkajsNamespace & { default?: typeof kafkajsNamespace }).default ?? kafkajsNamespace;

export const {
  AclOperationTypes,
  AclPermissionTypes,
  AclResourceTypes,
  CompressionCodecs,
  ConfigResourceTypes,
  ResourcePatternTypes,
} = kafkajs;
