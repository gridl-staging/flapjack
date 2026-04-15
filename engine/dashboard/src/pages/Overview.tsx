import { useState, useMemo, useCallback, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import api from '@/lib/api';
import { analyticsKeys } from '@/lib/queryKeys';
import { useIndexes, useDeleteIndex } from '@/hooks/useIndexes';
import { useHealth } from '@/hooks/useHealth';
import { useAnalyticsOverview, defaultRange, type DateRange } from '@/hooks/useAnalytics';
import { useExportIndex, useImportIndex } from '@/hooks/useSnapshots';
import { CreateIndexDialog } from '@/components/indexes/CreateIndexDialog';
import { ConfirmDialog } from '@/components/ui/confirm-dialog';
import {
  OverviewAnalyticsSection,
  OverviewHeader,
  OverviewIndexListSection,
  OverviewStatsCards,
} from './OverviewSections';

const ITEMS_PER_PAGE = 10;

export function Overview() {
  const { data: indexes, isLoading, error } = useIndexes();
  const { data: health, isLoading: healthLoading, error: healthError } = useHealth();
  const [currentPage, setCurrentPage] = useState(1);
  const [showCreateDialog, setShowCreateDialog] = useState(false);
  const deleteMutation = useDeleteIndex();
  const exportIndex = useExportIndex();
  const importIndex = useImportIndex();
  const [pendingDeleteIndex, setPendingDeleteIndex] = useState<string | null>(null);
  const navigate = useNavigate();
  const fileInputRef = useRef<HTMLInputElement>(null);
  const importTargetRef = useRef<string>('');

  const confirmDelete = useCallback(() => {
    if (pendingDeleteIndex) {
      deleteMutation.mutate(pendingDeleteIndex, {
        onSettled: () => setPendingDeleteIndex(null),
      });
    }
  }, [pendingDeleteIndex, deleteMutation]);

  const [showUploadDialog, setShowUploadDialog] = useState(false);
  const uploadFileRef = useRef<HTMLInputElement>(null);

  const handleImport = useCallback((indexName: string) => {
    importTargetRef.current = indexName;
    fileInputRef.current?.click();
  }, []);

  const handleTopLevelUpload = useCallback(() => {
    if (indexes?.length === 1) {
      // Only one index — import directly
      importTargetRef.current = indexes[0].uid;
      uploadFileRef.current?.click();
    } else if (indexes?.length) {
      setShowUploadDialog(true);
    }
  }, [indexes]);

  const handleUploadToIndex = useCallback((indexName: string) => {
    importTargetRef.current = indexName;
    setShowUploadDialog(false);
    uploadFileRef.current?.click();
  }, []);

  const onUploadFileSelected = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file && importTargetRef.current) {
      importIndex.mutate({ indexName: importTargetRef.current, file });
    }
    e.target.value = '';
  }, [importIndex]);

  const onFileSelected = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file && importTargetRef.current) {
      importIndex.mutate({ indexName: importTargetRef.current, file });
    }
    e.target.value = '';
  }, [importIndex]);

  const handleExportAll = useCallback(() => {
    indexes?.forEach((idx) => exportIndex.mutate(idx.uid));
  }, [indexes, exportIndex]);

  const queryClient = useQueryClient();
  const [showCleanupDialog, setShowCleanupDialog] = useState(false);
  const cleanupMutation = useMutation({
    mutationFn: async () => {
      const res = await api.post('/2/analytics/cleanup');
      return res.data;
    },
    onSuccess: () => {
      queryClient.resetQueries({ queryKey: analyticsKeys.all });
      setShowCleanupDialog(false);
    },
  });

  const analyticsRange: DateRange = useMemo(() => defaultRange(7), []);
  const { data: overview, isLoading: overviewLoading } = useAnalyticsOverview(analyticsRange);

  const totalDocs = indexes?.reduce((sum, idx) => sum + (idx.entries || 0), 0) || 0;
  const totalSize = indexes?.reduce((sum, idx) => sum + (idx.dataSize || 0), 0) || 0;

  // Pagination
  const totalPages = Math.ceil((indexes?.length || 0) / ITEMS_PER_PAGE);
  const paginatedIndexes = useMemo(() => {
    if (!indexes) return [];
    const start = (currentPage - 1) * ITEMS_PER_PAGE;
    return indexes.slice(start, start + ITEMS_PER_PAGE);
  }, [indexes, currentPage]);

  return (
    <div className="space-y-6">
      <input
        ref={fileInputRef}
        type="file"
        accept=".tar.gz,.tgz"
        className="hidden"
        onChange={onFileSelected}
        data-testid="overview-file-input"
      />
      <input
        ref={uploadFileRef}
        type="file"
        accept=".tar.gz,.tgz"
        className="hidden"
        onChange={onUploadFileSelected}
        data-testid="overview-upload-file-input"
      />
      <OverviewHeader
        hasIndexes={Boolean(indexes && indexes.length > 0)}
        importPending={importIndex.isPending}
        exportPending={exportIndex.isPending}
        onUpload={handleTopLevelUpload}
        onExportAll={handleExportAll}
        onCreateIndex={() => setShowCreateDialog(true)}
      />

      <OverviewStatsCards
        indexesCount={indexes?.length || 0}
        totalDocs={totalDocs}
        totalSize={totalSize}
        healthLoading={healthLoading}
        healthError={Boolean(healthError)}
        healthStatus={health?.status}
      />

      <OverviewAnalyticsSection
        overview={overview}
        overviewLoading={overviewLoading}
        indexes={indexes}
        cleanupPending={cleanupMutation.isPending}
        cleanupSuccess={cleanupMutation.isSuccess}
        onOpenCleanup={() => setShowCleanupDialog(true)}
      />

      <OverviewIndexListSection
        indexes={indexes}
        isLoading={isLoading}
        error={error}
        paginatedIndexes={paginatedIndexes}
        currentPage={currentPage}
        totalPages={totalPages}
        onPrevPage={() => setCurrentPage((page) => Math.max(1, page - 1))}
        onNextPage={() => setCurrentPage((page) => Math.min(totalPages, page + 1))}
        onNavigateToIndex={(indexName) => navigate(`/index/${encodeURIComponent(indexName)}`)}
        onImport={handleImport}
        onDeleteRequest={setPendingDeleteIndex}
        onExport={(indexName) => exportIndex.mutate(indexName)}
        exportPending={exportIndex.isPending}
        importPending={importIndex.isPending}
        deletePending={deleteMutation.isPending}
      />

      <CreateIndexDialog
        open={showCreateDialog}
        onOpenChange={setShowCreateDialog}
      />

      <ConfirmDialog
        open={pendingDeleteIndex !== null}
        onOpenChange={(open) => { if (!open) setPendingDeleteIndex(null); }}
        title="Delete Index"
        description={
          <>
            Are you sure you want to delete{' '}
            <code className="font-mono text-sm bg-muted px-1 py-0.5 rounded">
              {pendingDeleteIndex}
            </code>
            ? This action cannot be undone.
          </>
        }
        confirmLabel="Delete"
        variant="destructive"
        onConfirm={confirmDelete}
        isPending={deleteMutation.isPending}
      />

      <ConfirmDialog
        open={showCleanupDialog}
        onOpenChange={setShowCleanupDialog}
        title="Cleanup Analytics"
        description="This will remove analytics data for indexes that no longer exist. Analytics for your active indexes will not be affected."
        confirmLabel="Cleanup"
        onConfirm={() => cleanupMutation.mutate()}
        isPending={cleanupMutation.isPending}
      />

      {/* Upload index selection dialog */}
      <ConfirmDialog
        open={showUploadDialog}
        onOpenChange={setShowUploadDialog}
        title="Upload Snapshot"
        description={
          <div className="space-y-3">
            <p className="text-sm text-muted-foreground">Select an index to import the snapshot into:</p>
            <div className="space-y-1">
              {indexes?.map((idx) => (
                <button
                  key={idx.uid}
                  onClick={() => handleUploadToIndex(idx.uid)}
                  className="w-full text-left px-3 py-2 rounded-md border border-border hover:bg-accent transition-colors text-sm"
                >
                  <span className="font-medium">{idx.uid}</span>
                  <span className="text-muted-foreground ml-2">
                    ({(idx.entries || 0).toLocaleString()} docs)
                  </span>
                </button>
              ))}
            </div>
          </div>
        }
        confirmLabel="Cancel"
        onConfirm={() => setShowUploadDialog(false)}
      />
    </div>
  );
}
