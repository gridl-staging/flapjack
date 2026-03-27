import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, renderHook, waitFor } from '@testing-library/react';

vi.mock('@/lib/api', () => ({
  default: {
    post: vi.fn(),
  },
}));

import api from '@/lib/api';
import type { ChatResponse } from '@/lib/types';
import { useChat } from './useChat';

/** Fixture matching the backend ChatResponse shape from chat.rs. */
const STUB_CHAT_RESPONSE: ChatResponse = {
  answer: 'Based on your search for "headphones": content here',
  sources: [{ objectID: 'doc-1', content: 'Wireless headphones' }],
  conversationId: 'conv_abc123',
  queryID: 'q_xyz789',
};

const STUB_CHAT_RESPONSE_2: ChatResponse = {
  answer: 'Based on your search for "speaker": speaker content',
  sources: [{ objectID: 'doc-2', content: 'Bluetooth speaker' }],
  conversationId: 'conv_abc123',
  queryID: 'q_xyz790',
};

const STUB_RESPONSE = {
  data: STUB_CHAT_RESPONSE,
};

const STUB_RESPONSE_2 = {
  data: STUB_CHAT_RESPONSE_2,
};

describe('useChat', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('returns empty initial state', () => {
    const { result } = renderHook(() => useChat('test-index'));

    expect(result.current.messages).toEqual([]);
    expect(result.current.isLoading).toBe(false);
    expect(result.current.error).toBeNull();
  });

  it('appends user message optimistically and sets isLoading on sendMessage', async () => {
    // Arrange: mock a delayed response so we can observe the loading state
    let resolvePost: (value: typeof STUB_RESPONSE) => void;
    vi.mocked(api.post).mockImplementation(
      () => new Promise((resolve) => { resolvePost = resolve; }),
    );

    const { result } = renderHook(() => useChat('test-index'));

    // Act: send a message (don't await yet)
    act(() => {
      result.current.sendMessage('headphones');
    });

    // Assert: user message appended optimistically, loading started
    expect(result.current.messages).toHaveLength(1);
    expect(result.current.messages[0]).toMatchObject({
      role: 'user',
      content: 'headphones',
    });
    expect(result.current.isLoading).toBe(true);

    // Resolve the API call
    await act(async () => {
      resolvePost!(STUB_RESPONSE);
    });

    // Assert: assistant message appended with sources, loading stopped
    expect(result.current.messages).toHaveLength(2);
    expect(result.current.messages[1]).toMatchObject({
      role: 'assistant',
      content: STUB_RESPONSE.data.answer,
      sources: STUB_RESPONSE.data.sources,
    });
    expect(result.current.isLoading).toBe(false);
    expect(result.current.error).toBeNull();
  });

  it('reuses conversationId from first response in subsequent requests', async () => {
    vi.mocked(api.post)
      .mockResolvedValueOnce(STUB_RESPONSE)
      .mockResolvedValueOnce(STUB_RESPONSE_2);

    const { result } = renderHook(() => useChat('test-index'));

    // First message
    await act(async () => {
      await result.current.sendMessage('headphones');
    });

    // Second message — should reuse conversationId
    await act(async () => {
      await result.current.sendMessage('speaker');
    });

    // Assert: second API call included conversationId from first response
    expect(api.post).toHaveBeenCalledTimes(2);
    const secondCallArgs = vi.mocked(api.post).mock.calls[1];
    expect(secondCallArgs[1]).toMatchObject({
      conversationId: 'conv_abc123',
    });

    // Assert: all 4 messages present (2 user + 2 assistant)
    expect(result.current.messages).toHaveLength(4);
    expect(result.current.messages[0]).toMatchObject({ role: 'user', content: 'headphones' });
    expect(result.current.messages[1]).toMatchObject({ role: 'assistant', content: STUB_RESPONSE.data.answer });
    expect(result.current.messages[2]).toMatchObject({ role: 'user', content: 'speaker' });
    expect(result.current.messages[3]).toMatchObject({ role: 'assistant', content: STUB_RESPONSE_2.data.answer });
  });

  it('resets chat state and ignores stale responses when indexName changes', async () => {
    let resolveFirstPost: ((value: typeof STUB_RESPONSE) => void) | undefined;
    vi.mocked(api.post)
      .mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            resolveFirstPost = resolve;
          }),
      )
      .mockResolvedValueOnce(STUB_RESPONSE_2);

    const { result, rerender } = renderHook(
      ({ indexName }) => useChat(indexName),
      { initialProps: { indexName: 'products' } },
    );

    act(() => {
      void result.current.sendMessage('headphones');
    });

    expect(result.current.messages).toEqual([
      { role: 'user', content: 'headphones' },
    ]);
    expect(result.current.isLoading).toBe(true);

    rerender({ indexName: 'orders' });

    await waitFor(() => {
      expect(result.current.messages).toEqual([]);
      expect(result.current.isLoading).toBe(false);
      expect(result.current.error).toBeNull();
    });

    await act(async () => {
      resolveFirstPost!(STUB_RESPONSE);
    });

    expect(result.current.messages).toEqual([]);
    expect(result.current.isLoading).toBe(false);
    expect(result.current.error).toBeNull();

    await act(async () => {
      await result.current.sendMessage('speaker');
    });

    expect(api.post).toHaveBeenNthCalledWith(
      2,
      '/1/indexes/orders/chat',
      expect.objectContaining({
        query: 'speaker',
        conversationId: undefined,
      }),
    );
    expect(result.current.messages).toHaveLength(2);
    expect(result.current.messages[0]).toMatchObject({
      role: 'user',
      content: 'speaker',
    });
    expect(result.current.messages[1]).toMatchObject({
      role: 'assistant',
      content: STUB_RESPONSE_2.data.answer,
    });
  });

  it('sets error on API rejection without dropping the user message', async () => {
    const networkError = new Error('Network error');
    vi.mocked(api.post).mockRejectedValue(networkError);

    const { result } = renderHook(() => useChat('test-index'));

    await act(async () => {
      await result.current.sendMessage('headphones');
    });

    // Assert: error is set, loading stopped
    expect(result.current.error).toBe(networkError);
    expect(result.current.isLoading).toBe(false);

    // Assert: user message is preserved (not silently dropped)
    expect(result.current.messages).toHaveLength(1);
    expect(result.current.messages[0]).toMatchObject({
      role: 'user',
      content: 'headphones',
    });
  });

  it('encodes the index name when calling the chat endpoint', async () => {
    vi.mocked(api.post).mockResolvedValue(STUB_RESPONSE);

    const { result } = renderHook(() => useChat('products/2026?beta'));

    await act(async () => {
      await result.current.sendMessage('headphones');
    });

    expect(api.post).toHaveBeenCalledWith(
      `/1/indexes/${encodeURIComponent('products/2026?beta')}/chat`,
      expect.objectContaining({ query: 'headphones' }),
    );
  });
});
