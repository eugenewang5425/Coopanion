/**
 * A ResponseAssembly that tolerates Ollama's stream shape (checked against 0.34.4 and 0.35.1).
 * Ollama announces the message and each function_call under the same `output_index` (0), skips
 * the message's `output_item.done`, and lists the terminal `response.output` in reverse. The
 * strict Responses accumulator (`vendor/cortico/src/protocol/open-responses/stream.ts`) rejects
 * all of that, so events are rewritten per item id before they reach it: a repeated index with
 * a new item id is seated at the next free index, every later event for that id follows its
 * assigned index, and the terminal output array is put back into accumulated order.
 */
import { NativeResponseAssembly, type ResponseAssembly } from 'cortico/providers/transport/response-assembly.ts';
import type { Response, StreamEvent } from 'cortico/protocol/open-responses/index.ts';

export class OllamaStreamFix implements ResponseAssembly {
  private inner = new NativeResponseAssembly();
  private indexById = new Map<string, number>();
  private maxIndex = -1;
  private remapped = false;

  feed(payload: unknown, emit: (event: StreamEvent) => void): void {
    this.inner.feed(this.normalize(payload), emit);
  }

  finish(emit: (event: StreamEvent) => void): Response {
    return this.inner.finish();
  }
  snapshot(): Response | null {
    return this.inner.snapshot();
  }
  meters() {
    return this.inner.meters();
  }
  serviceTier(): string | null {
    return this.inner.serviceTier();
  }

  private normalize(payload: unknown): unknown {
    if (!payload || typeof payload !== 'object') return payload;
    const e = payload as Record<string, unknown>;
    const type = typeof e.type === 'string' ? e.type : '';
    if (type === 'response.output_item.added') {
      const id = (e.item as { id?: unknown } | null)?.id;
      const idx = e.output_index;
      if (typeof id === 'string' && typeof idx === 'number' && !this.indexById.has(id)) {
        if (idx <= this.maxIndex) {
          const next = this.maxIndex + 1;
          this.maxIndex = next;
          this.indexById.set(id, next);
          this.remapped = true;
          return { ...e, output_index: next };
        }
        this.maxIndex = idx;
        this.indexById.set(id, idx);
      }
      return e;
    }
    const id = (typeof e.item_id === 'string' ? e.item_id : (e.item as { id?: unknown } | null)?.id) as string | undefined;
    if (id && this.indexById.has(id)) {
      const to = this.indexById.get(id)!;
      if (e.output_index !== to) return { ...e, output_index: to };
      return e;
    }
    if (this.remapped && 'response' in e) {
      const out = (e.response as { output?: Array<{ id?: unknown }> | null } | null)?.output;
      if (Array.isArray(out)) {
        let disordered = false;
        const reordered = [...out];
        for (const item of out) {
          const to = item && typeof item.id === 'string' ? this.indexById.get(item.id) : undefined;
          if (to === undefined) continue;
          if (out.indexOf(item) !== to) disordered = true;
          reordered[to] = item;
        }
        if (disordered) return { ...e, response: { ...(e.response as Record<string, unknown>), output: reordered } };
      }
    }
    return e;
  }
}
