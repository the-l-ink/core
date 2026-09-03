export type Bytes = Uint8Array<ArrayBuffer>

export type Serialize = (value: unknown) => Bytes

export type Deserialize = (bytes: Bytes) => unknown

const encoder = new TextEncoder()

const decoder = new TextDecoder("utf-8", { fatal: true })

export const serializeJSON: Serialize = value => {

    const serialized = JSON.stringify(value)

    if (serialized === undefined) throw new TypeError("The value cannot be serialized as JSON")

    return encoder.encode(serialized)
}

export const deserializeJSON: Deserialize = bytes => JSON.parse(decoder.decode(bytes))
