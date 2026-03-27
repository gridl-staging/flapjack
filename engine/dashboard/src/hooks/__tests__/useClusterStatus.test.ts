import React from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

vi.mock("@/lib/api", () => ({
  default: {
    get: vi.fn(),
  },
}));

import api from "@/lib/api";
import {
  useClusterStatus,
  type ClusterStatusResponse,
  type HAClusterStatus,
  type StandaloneClusterStatus,
} from "@/hooks/useClusterStatus";

const STANDALONE_PAYLOAD: StandaloneClusterStatus = {
  node_id: "node-standalone",
  replication_enabled: false,
  peers: [],
};

const HA_PAYLOAD: HAClusterStatus = {
  node_id: "node-a",
  replication_enabled: true,
  peers_total: 2,
  peers_healthy: 1,
  peers: [
    {
      peer_id: "node-b",
      addr: "http://node-b:7700",
      status: "healthy",
      last_success_secs_ago: 5,
    },
    {
      peer_id: "node-c",
      addr: "http://node-c:7700",
      status: "circuit_open",
      last_success_secs_ago: null,
    },
  ],
};

function createWrapper() {
  const client = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
        retryDelay: 0,
      },
    },
  });

  return {
    client,
    wrapper: ({ children }: { children: React.ReactNode }) =>
      React.createElement(QueryClientProvider, { client }, children),
  };
}

describe("useClusterStatus", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("returns standalone cluster_status payload unchanged", async () => {
    vi.mocked(api.get).mockResolvedValue({
      data: STANDALONE_PAYLOAD,
    } as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useClusterStatus(), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    const response: ClusterStatusResponse | undefined = result.current.data;
    expect(response).toEqual(STANDALONE_PAYLOAD);
    expect(response?.replication_enabled).toBe(false);
    expect(response?.peers).toEqual([]);
    expect(response).not.toHaveProperty("peers_total");
    expect(response).not.toHaveProperty("peers_healthy");
  });

  it("returns HA cluster_status payload unchanged", async () => {
    vi.mocked(api.get).mockResolvedValue({
      data: HA_PAYLOAD,
    } as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useClusterStatus(), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    const response: ClusterStatusResponse | undefined = result.current.data;
    expect(response).toEqual(HA_PAYLOAD);
    expect(response?.replication_enabled).toBe(true);
    if (response?.replication_enabled) {
      expect(response).toHaveProperty("peers_total");
      expect(response).toHaveProperty("peers_healthy");
      expect(response.peers_total).toBe(2);
      expect(response.peers_healthy).toBe(1);
      expect(response.peers[0]).toEqual({
        peer_id: "node-b",
        addr: "http://node-b:7700",
        status: "healthy",
        last_success_secs_ago: 5,
      });
    }
  });

  it("calls /internal/cluster/status and uses the cluster-status query contract", async () => {
    vi.mocked(api.get).mockResolvedValue({
      data: HA_PAYLOAD,
    } as never);

    const { client, wrapper } = createWrapper();
    const { result } = renderHook(() => useClusterStatus(), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    expect(api.get).toHaveBeenCalledWith("/internal/cluster/status");
    expect(client.getQueryData(["cluster-status"])).toEqual(HA_PAYLOAD);
    expect(client.getQueryData(["health-detail"])).toBeUndefined();

    const clusterQuery = client.getQueryCache().find({
      queryKey: ["cluster-status"],
    });
    expect(clusterQuery?.options.queryKey).toEqual(["cluster-status"]);
    expect(clusterQuery?.options.refetchInterval).toBe(5000);
    expect(clusterQuery?.options.retry).toBe(1);
  });

  it("surfaces request failures after one retry", async () => {
    const error = new Error("cluster endpoint unavailable");
    vi.mocked(api.get).mockRejectedValue(error as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useClusterStatus(), { wrapper });

    await waitFor(() => expect(result.current.isError).toBe(true));

    expect(result.current.error).toBe(error);
    expect(api.get).toHaveBeenCalledWith("/internal/cluster/status");
    expect(api.get).toHaveBeenCalledTimes(2);
  });

  it("rejects malformed HA payloads before they reach the page", async () => {
    vi.mocked(api.get).mockResolvedValue({
      data: {
        node_id: "node-a",
        replication_enabled: true,
        peers_healthy: 1,
        peers: [],
      },
    } as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useClusterStatus(), { wrapper });

    await waitFor(() => expect(result.current.isError).toBe(true));

    expect(result.current.error).toBeInstanceOf(Error);
    expect((result.current.error as Error).message).toContain("peers_total");
    expect(api.get).toHaveBeenCalledWith("/internal/cluster/status");
    expect(api.get).toHaveBeenCalledTimes(2);
  });
});
