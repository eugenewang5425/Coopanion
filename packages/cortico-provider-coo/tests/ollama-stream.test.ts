import { describe, expect, it } from 'vitest';
import { ResponseAccumulator, ResponseProtocolError } from 'cortico/protocol/open-responses/stream.ts';
import { OllamaStreamFix } from '../src/ollama-stream.ts';

type Event = Record<string, unknown>;
const emit = () => {};
/** Renumbers `sequence_number`, which the accumulator requires to grow strictly. */
const withSeq = (events: Event[]): Event[] => events.map((e, i) => ({ ...e, sequence_number: i }));

/** Ollama's observed shape: message and function_call both announced at output_index 0, no
    output_item.done for the message, and the terminal output listed function-call first. */
const created = { type: 'response.created', response: { id: 'resp_1', model: 'm', status: 'in_progress', output: [] } };
const addedMsg = { type: 'response.output_item.added', output_index: 0, item: { id: 'msg_1', type: 'message', status: 'in_progress', role: 'assistant', content: [] } };
const partAdded = { type: 'response.content_part.added', item_id: 'msg_1', output_index: 0, content_index: 0, part: { type: 'output_text', text: '', annotations: [] } };
const textDelta = { type: 'response.output_text.delta', item_id: 'msg_1', output_index: 0, content_index: 0, delta: '你好' };
const addedCall = { type: 'response.output_item.added', output_index: 0, item: { id: 'fc_1', type: 'function_call', status: 'in_progress', call_id: 'call_1', name: 'pet_act', arguments: '' } };
const argsDelta = { type: 'response.function_call_arguments.delta', item_id: 'fc_1', output_index: 0, delta: '{"actions":["turn"]}' };
const argsDone = { type: 'response.function_call_arguments.done', item_id: 'fc_1', output_index: 0, arguments: '{"actions":["turn"]}' };
const doneCall = { type: 'response.output_item.done', output_index: 0, item: { id: 'fc_1', type: 'function_call', status: 'completed', call_id: 'call_1', name: 'pet_act', arguments: '{"actions":["turn"]}' } };
const completed = {
  type: 'response.completed',
  response: {
    id: 'resp_1', model: 'm', status: 'completed',
    usage: { input_tokens: 1, output_tokens: 2, total_tokens: 3 },
    output: [
      { id: 'fc_1', type: 'function_call', status: 'completed', call_id: 'call_1', name: 'pet_act', arguments: '{"actions":["turn"]}' },
      { id: 'msg_1', type: 'message', status: 'completed', role: 'assistant', content: [{ type: 'output_text', text: '你好', annotations: [] }] },
    ],
  },
};
const ollamaStream = withSeq([created, addedMsg, partAdded, textDelta, addedCall, argsDelta, argsDone, doneCall, completed]);

type SimpleResponse = { output: Array<{ id: string; type: string; content?: Array<{ text?: string }>; arguments?: string }> };

describe('OllamaStreamFix', () => {
  it('the raw stream really is rejected by the strict accumulator', () => {
    const acc = new ResponseAccumulator();
    expect(() => { for (const e of ollamaStream) acc.accept(e as never); }).toThrow(new ResponseProtocolError('Duplicate output item'));
  });

  it('rewrites indexes so the strict accumulator accepts Ollama\'s stream', () => {
    const fix = new OllamaStreamFix();
    for (const e of ollamaStream) fix.feed(e as never, emit as never);
    const out = fix.finish(emit as never) as unknown as SimpleResponse;
    expect(out.output.map((o) => [o.id, o.type])).toEqual([['msg_1', 'message'], ['fc_1', 'function_call']]);
    const msg = out.output[0];
    expect(msg.content?.[0].text).toBe('你好');
    const call = out.output[1];
    expect(JSON.parse(call.arguments ?? '{}')).toEqual({ actions: ['turn'] });
  });

  it('leaves a conforming stream untouched', () => {
    // the same conversation as a well-formed service would send it: distinct indexes, the
    // message closed, and the terminal output already in announcement order
    const conforming = withSeq([
      created,
      addedMsg, partAdded, textDelta,
      { ...addedCall, output_index: 1 }, { ...argsDelta, output_index: 1 }, { ...argsDone, output_index: 1 },
      { ...doneCall, output_index: 1 },
      { type: 'response.output_item.done', output_index: 0, item: { id: 'msg_1', type: 'message', status: 'completed', role: 'assistant', content: [{ type: 'output_text', text: '你好', annotations: [] }] } },
      { ...completed, response: { ...(completed.response as Event), output: (completed.response as { output: Event[] }).output.slice().reverse() } },
    ]);
    const fix = new OllamaStreamFix();
    for (const e of conforming) fix.feed(e as never, emit as never);
    const out = fix.finish(emit as never) as unknown as SimpleResponse;
    expect(out.output.map((o) => o.id)).toEqual(['msg_1', 'fc_1']);
  });
});
