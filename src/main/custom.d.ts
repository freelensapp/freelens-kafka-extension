// Modules of the main process's libraries that are imported by path and ship no declaration. Each is CommonJS: its
// default export is its `module.exports`, which the importing file types where it uses it.

/** lz4-asm's asm.js build: a function that initializes the codec (the package's default entry loads a WASM file). */
declare module "lz4-asm/dist/lz4asm.js" {
  const initialize: unknown;
  export default initialize;
}

/** kafkajs internals: the protocol encoder and decoder, and the record batch reader of the consumer. */
declare module "kafkajs/src/protocol/encoder.js" {
  const Encoder: unknown;
  export default Encoder;
}

declare module "kafkajs/src/protocol/decoder.js" {
  const Decoder: unknown;
  export default Decoder;
}

declare module "kafkajs/src/consumer/batch.js" {
  const Batch: unknown;
  export default Batch;
}
