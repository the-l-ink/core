export type Bytes = Uint8Array<ArrayBuffer>

/**
 * Turns streams into references and back for one boundary, such as a `StreamRelay`.
 */
export interface StreamReferences {

    /** Registers a local stream to be sent and returns its reference. */
    export(stream: ReadableStream<unknown>): string

    /** Returns the local stream for a received reference. */
    import(reference: string): ReadableStream<unknown>
}

/**
 * What a boundary supplies to its codec beside the value or the bytes.
 */
export interface CodecOptions {

    /** Carries the streams met in the value; a codec that cannot carry streams ignores it. */
    streams?: StreamReferences
}

export type Serialize = (value: unknown, options?: CodecOptions) => Bytes

export type Deserialize = (bytes: Bytes, options?: CodecOptions) => unknown

const encoder = new TextEncoder()

const decoder = new TextDecoder("utf-8", { fatal: true })

export const serializeJSON: Serialize = value => {

    const serialized = JSON.stringify(value)

    if (serialized === undefined) throw new TypeError("The value cannot be serialized as JSON")

    return encoder.encode(serialized)
}

export const deserializeJSON: Deserialize = bytes => JSON.parse(decoder.decode(bytes))
