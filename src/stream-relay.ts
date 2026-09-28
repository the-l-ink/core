import { v4 as uuidv4 } from "uuid"
import type { StreamReferences } from "./codec.js"

/**
 * One message of the stream relay protocol.
 *
 * - `pull` grants the sender credit for more chunks.
 * - `chunk` carries one chunk.
 * - `end` closes the stream after its last chunk.
 * - `error` fails the stream on the receiving side.
 * - `cancel` stops the sender because the receiver no longer reads.
 */
export type StreamMessage =
    | readonly ["pull", string, number]
    | readonly ["chunk", string, unknown]
    | readonly ["end", string]
    | readonly ["error", string, string]
    | readonly ["cancel", string, string]

/**
 * Stream relay tuning.
 */
export interface StreamRelayOptions {

    /**
     * Chunks a receiver lets arrive before it reads them. Defaults to 16.
     */
    window?: number

    /**
     * Largest byte chunk sent as one message; larger byte chunks are split. Defaults to 64 KiB.
     */
    chunkBytes?: number

    /**
     * Milliseconds an exported stream waits for its first pull before it is canceled. Defaults to 60 seconds.
     */
    idle?: number
}

interface Exported {
    reader: ReadableStreamDefaultReader<unknown>
    credit: number
    remainder: Uint8Array | null
    pumping: boolean
    timer: ReturnType<typeof setTimeout> | null
}

interface Imported {
    controller: ReadableStreamDefaultController<unknown>
    requested: number
    arrived: (() => void) | null
}

/**
 * Carries ReadableStreams across one boundary as references.
 *
 * A boundary exports a local stream to obtain a reference to send in its
 * payload, and imports a received reference to obtain a local stream. The
 * chunks then travel as relay messages over whatever channel the boundary
 * already uses, delivered through `receive()`. Data flows only when the
 * receiver pulls: the receiver grants credit, and the sender reads from its
 * stream only as far as that credit allows, so neither side buffers without
 * bound. Canceling the imported stream stops the sender; ending, failing, or
 * closing the relay reaches the other side.
 *
 * The relay does not know the channel or how messages are serialized.
 */
export class StreamRelay implements StreamReferences {

    /**
     * Local streams this side sends, by reference.
     */
    private readonly exported = new Map<string, Exported>()

    /**
     * Remote streams this side receives, by reference.
     */
    private readonly imported = new Map<string, Imported>()

    private readonly window: number

    private readonly chunkBytes: number

    private readonly idle: number

    /**
     * Error that ended this relay, after `close()`.
     */
    private closed: Error | null = null

    /**
     * Initialize a relay that sends its messages through one channel.
     *
     * @param send Delivers one relay message to the other side
     * @param options Relay tuning
     */
    public constructor(private readonly send: (message: StreamMessage) => void, options: StreamRelayOptions = {}) {

        this.window = positive(options.window ?? 16, "window")
        this.chunkBytes = positive(options.chunkBytes ?? 64 * 1024, "chunkBytes")
        this.idle = positive(options.idle ?? 60_000, "idle")
    }

    /**
     * Whether a value is a stream relay message.
     */
    public static isMessage(value: unknown): value is StreamMessage {

        if (!Array.isArray(value) || typeof value[1] !== "string") return false

        switch (value[0]) {

            case "pull": return value.length === 3 && Number.isSafeInteger(value[2]) && value[2] > 0
            case "chunk": return value.length === 3
            case "end": return value.length === 2
            case "error":
            case "cancel": return value.length === 3 && typeof value[2] === "string"
            default: return false
        }
    }

    /**
     * Register a local stream to be sent, and return its reference.
     *
     * The stream is locked from now on; nothing is read until the other side pulls.
     *
     * @param stream Stream whose chunks are sent
     * @returns Reference to place in the payload
     */
    public export(stream: ReadableStream<unknown>): string {

        if (this.closed) throw this.closed

        const id = uuidv4()
        const entry: Exported = { reader: stream.getReader(), credit: 0, remainder: null, pumping: false, timer: null }

        // A reference nobody pulls, such as one in a payload the receiver ignored, must not hold the stream open.
        // The other side may still hold the reference, so it learns the stream ended.
        entry.timer = setTimeout(() => this.cancelExport(id, new Error("The stream was not read in time"), true), this.idle)

        this.exported.set(id, entry)

        return id
    }

    /**
     * Create the local stream for a received reference.
     *
     * @param id Reference received in a payload
     * @returns Stream that pulls its chunks from the other side
     */
    public import(id: string): ReadableStream<unknown> {

        if (this.closed) throw this.closed

        if (this.imported.has(id)) throw new Error("The stream reference was already imported")

        const relay = this

        return new ReadableStream<unknown>({

            start(controller) {

                relay.imported.set(id, { controller, requested: 0, arrived: null })
            },

            pull(controller) {

                const entry = relay.imported.get(id)

                // The stream already ended; a runtime that pulls it again must not be answered at once, or it pulls forever.
                if (!entry) return new Promise<void>(() => undefined)

                // Grant what the consumer is ready for beyond what is already on its way.
                const grant = Math.max(0, (controller.desiredSize ?? 0) - entry.requested)

                if (grant > 0) {

                    entry.requested += grant
                    relay.send(["pull", id, grant])
                }

                // Resolve once something arrives, so the stream asks again only when it can use more.
                return new Promise<void>(resolve => { entry.arrived = resolve })
            },

            cancel(reason) {

                if (!relay.imported.delete(id)) return

                relay.send(["cancel", id, message(reason, "The stream was canceled")])
            }

        }, new CountQueuingStrategy({ highWaterMark: this.window }))
    }

    /**
     * Handle one relay message received from the other side.
     *
     * @param value Received relay message
     */
    public receive(value: unknown) {

        if (this.closed || !StreamRelay.isMessage(value)) return

        const [operation, id] = value

        switch (value[0]) {

            case "pull": {

                const entry = this.exported.get(id)

                // The stream already ended here, such as one that waited too long to be read: say so, or the reader waits forever.
                if (!entry) {

                    this.send(["error", id, "The stream is no longer available"])

                    return
                }

                if (entry.timer) {

                    clearTimeout(entry.timer)
                    entry.timer = null
                }

                entry.credit += value[2]
                void this.pump(id, entry)

                return
            }

            case "cancel": {

                this.cancelExport(id, new Error(value[2]), false)

                return
            }

            default: {

                const entry = this.imported.get(id)

                // Chunks for a stream this side never imported, or already canceled, are refused.
                if (!entry) {

                    if (operation === "chunk") this.send(["cancel", id, "The stream is not being read"])

                    return
                }

                if (value[0] === "chunk") {

                    entry.requested = Math.max(0, entry.requested - 1)
                    entry.controller.enqueue(value[2])

                } else {

                    this.imported.delete(id)

                    if (value[0] === "end") entry.controller.close()
                    else entry.controller.error(new Error(value[2]))
                }

                const arrived = entry.arrived

                entry.arrived = null
                arrived?.()
            }
        }
    }

    /**
     * End every stream crossing this boundary, such as when its channel closes.
     *
     * @param reason Why the relay ended
     */
    public close(reason: unknown = new Error("The stream relay was closed")) {

        if (this.closed) return

        this.closed = reason instanceof Error ? reason : new Error(message(reason, "The stream relay was closed"))

        for (const [id, entry] of this.imported) {

            this.imported.delete(id)
            entry.controller.error(this.closed)
            entry.arrived?.()
        }

        for (const id of [...this.exported.keys()]) this.cancelExport(id, this.closed, false)
    }

    /**
     * Send chunks while the receiver's credit lasts.
     */
    private async pump(id: string, entry: Exported) {

        if (entry.pumping) return

        entry.pumping = true

        try {

            while (entry.credit > 0 && this.exported.get(id) === entry) {

                let chunk: unknown

                if (entry.remainder) {

                    chunk = entry.remainder

                } else {

                    const result = await entry.reader.read()

                    if (this.exported.get(id) !== entry) return

                    if (result.done) {

                        this.exported.delete(id)
                        this.send(["end", id])

                        return
                    }

                    chunk = result.value
                }

                // Large byte chunks travel as several messages, so no single message holds the channel for long.
                if (chunk instanceof Uint8Array && chunk.byteLength > this.chunkBytes) {

                    entry.remainder = chunk.subarray(this.chunkBytes)
                    chunk = chunk.subarray(0, this.chunkBytes)

                } else {

                    entry.remainder = null
                }

                entry.credit--
                this.send(["chunk", id, chunk])
            }

        } catch (exception) {

            if (this.exported.get(id) !== entry) return

            this.exported.delete(id)
            this.send(["error", id, message(exception, "The stream failed")])

        } finally {

            entry.pumping = false
        }
    }

    /**
     * Stop sending one stream and release its source.
     *
     * @param notify Whether the other side must be told the stream ended
     */
    private cancelExport(id: string, reason: Error, notify: boolean) {

        const entry = this.exported.get(id)

        if (!entry) return

        this.exported.delete(id)

        if (entry.timer) clearTimeout(entry.timer)

        entry.reader.cancel(reason).catch(() => undefined)

        if (notify) this.send(["error", id, reason.message])
    }
}

function positive(value: number, name: string) {

    if (!Number.isSafeInteger(value) || value < 1) throw new RangeError(`${name} must be a positive integer`)

    return value
}

function message(reason: unknown, fallback: string) {

    return reason instanceof Error ? reason.message : typeof reason === "string" && reason ? reason : fallback
}
