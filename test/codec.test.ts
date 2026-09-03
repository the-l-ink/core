import { describe, expect, test } from "bun:test"
import { deserializeJSON, serializeJSON } from "../src/core.js"

describe("JSON byte utilities", () => {

    test("serialize JSON as UTF-8 bytes", () => {

        const bytes = serializeJSON({ ready: true })

        expect(bytes).toBeInstanceOf(Uint8Array)
        expect(new TextDecoder().decode(bytes)).toBe('{"ready":true}')
    })

    test("deserialize UTF-8 JSON bytes", () => {

        expect(deserializeJSON(new TextEncoder().encode('["ready",42]'))).toEqual(["ready", 42])
    })

    test("reject values JSON cannot represent", () => {

        expect(() => serializeJSON(undefined)).toThrow("cannot be serialized as JSON")
    })
})
