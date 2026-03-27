/**
 * @module React Query mutation hooks for deleting a single document and batch-adding documents to a Flapjack search index, with toast notifications and background task polling.
 */
import { useMutation, useQueryClient } from '@tanstack/react-query';
import api from '@/lib/api';
import { useToast } from '@/hooks/use-toast';
import { addActiveTask, removeActiveTask } from '@/hooks/useIndexingStatus';

interface BatchRequest {
  action: 'addObject';
  body: Record<string, unknown>;
}

interface BatchResponse {
  taskID: number;
  objectIDs: string[];
}

/**
 * React Query mutation hook that deletes a single document from an index by objectID.
 * Invalidates search and indexes queries on success and shows a toast notification.
 * @param indexName - Name of the index to delete from.
 * @returns A mutation whose `mutate` accepts the objectID string to delete.
 */
export function useDeleteDocument(indexName: string) {
  const queryClient = useQueryClient();
  const { toast } = useToast();

  return useMutation<void, Error, string>({
    mutationFn: async (objectID) => {
      await api.delete(`/1/indexes/${indexName}/${encodeURIComponent(objectID)}`);
    },
    onSuccess: (_data, objectID) => {
      queryClient.invalidateQueries({ queryKey: ['search'] });
      queryClient.invalidateQueries({ queryKey: ['indexes'] });
      toast({
        title: 'Document deleted',
        description: `"${objectID}" has been removed.`,
      });
    },
    onError: (error) => {
      toast({
        variant: 'destructive',
        title: 'Failed to delete document',
        description: error.message,
      });
    },
  });
}

/**
 * React Query mutation hook that batch-adds documents to an index via the batch API.
 * On success, registers the returned taskID as an active indexing task, displays a persistent toast, and polls the task endpoint every 500 ms until the status is `published` or `error` (with a 30 s safety timeout). Query caches for search, indexes, and index-stats are invalidated once indexing completes.
 * @param indexName - Name of the target index.
 * @returns A mutation whose `mutate` accepts an array of document objects to index.
 */
export function useAddDocuments(indexName: string) {
  const queryClient = useQueryClient();
  const { toast } = useToast();

  return useMutation<BatchResponse, Error, Record<string, unknown>[]>({
    mutationFn: async (documents) => {
      const requests: BatchRequest[] = documents.map((doc) => ({
        action: 'addObject',
        body: doc,
      }));
      const { data } = await api.post(`/1/indexes/${indexName}/batch`, { requests });
      return data;
    },
    onSuccess: (data, documents) => {
      queryClient.invalidateQueries({ queryKey: ['search'] });
      queryClient.invalidateQueries({ queryKey: ['indexes'] });

      const taskID = data.taskID;

      // Track as active task for the header indicator
      addActiveTask({
        taskID,
        indexName,
        documentCount: documents.length,
        startedAt: Date.now(),
      });

      // Show persistent toast while indexing
      const { dismiss } = toast({
        title: 'Indexing...',
        description: `${documents.length} document(s) being indexed.`,
        duration: 0, // stay visible
      });

      // Poll task status until complete
      const poll = setInterval(async () => {
        try {
          const { data: task } = await api.get(`/1/tasks/${taskID}`);
          if (task.status === 'published') {
            clearInterval(poll);
            removeActiveTask(taskID);
            // Re-invalidate to pick up newly indexed docs
            queryClient.invalidateQueries({ queryKey: ['search'] });
            queryClient.invalidateQueries({ queryKey: ['indexes'] });
            queryClient.invalidateQueries({ queryKey: ['index-stats'] });
            dismiss();
            const rejected = task.rejected_count || 0;
            const indexed = task.indexed_documents || documents.length;
            toast({
              title: 'Indexing complete',
              description: rejected > 0
                ? `${indexed} indexed, ${rejected} rejected.`
                : `${indexed} document(s) indexed successfully.`,
            });
          } else if (task.status === 'error') {
            clearInterval(poll);
            removeActiveTask(taskID);
            dismiss();
            toast({
              variant: 'destructive',
              title: 'Indexing failed',
              description: task.error || 'Unknown error during indexing.',
            });
          }
        } catch {
          // Task might have been evicted; stop polling
          clearInterval(poll);
          removeActiveTask(taskID);
        }
      }, 500);

      // Safety timeout — stop polling after 30s
      setTimeout(() => {
        clearInterval(poll);
        removeActiveTask(taskID);
      }, 30000);
    },
    onError: (error) => {
      toast({
        variant: 'destructive',
        title: 'Failed to add documents',
        description: error.message,
      });
    },
  });
}
