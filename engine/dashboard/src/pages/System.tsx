import { memo, useRef, type ChangeEvent } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Skeleton } from '@/components/ui/skeleton';
import { useHealthDetail, useInternalStatus } from '@/hooks/useSystemStatus';
import { useIndexes } from '@/hooks/useIndexes';
import {
  useExportIndex,
  useImportIndex,
  useSnapshotToS3,
  useRestoreFromS3,
  useListSnapshots,
} from '@/hooks/useSnapshots';
import {
  Activity,
  Server,
  CheckCircle,
  XCircle,
} from 'lucide-react';
import {
  createHealthStats,
  HealthStatsGrid,
  IndexDetailsTable,
  IndexesOverviewCards,
  IndexesPendingTasksNotice,
  IndexHealthSummary,
  interpretIndexes,
  SnapshotsLocalCard,
  SnapshotsS3Card,
  StatusMeaningGuidance,
} from './SystemTabSections';

function HealthTab() {
  const { data, isLoading, isError, error } = useHealthDetail();

  if (isLoading) {
    return (
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {Array.from({ length: 6 }).map((_, i) => (
          <Card key={i}><CardContent className="pt-6"><Skeleton className="h-16" /></CardContent></Card>
        ))}
      </div>
    );
  }

  if (isError) {
    return (
      <Card>
        <CardContent className="pt-6">
          <div className="flex items-center gap-3 text-destructive">
            <XCircle className="h-5 w-5" />
            <div>
              <p className="font-medium">Failed to fetch health status</p>
              <p className="text-sm text-muted-foreground">{(error as Error)?.message}</p>
            </div>
          </div>
        </CardContent>
      </Card>
    );
  }

  const heapMb = data?.heap_allocated_mb ?? 0;
  const limitMb = data?.system_limit_mb ?? 0;
  const memPercent = limitMb > 0 ? Math.round((heapMb / limitMb) * 100) : 0;

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3">
        <p className="text-sm text-muted-foreground">Auto-refreshes every 5 seconds</p>
        {data?.version && (
          <span
            className="inline-flex items-center rounded-full bg-muted px-2.5 py-0.5 text-xs font-medium"
            data-testid="health-version"
          >
            {data.version}{data.build_profile ? ` · ${data.build_profile}` : ''}
          </span>
        )}
      </div>
      <HealthStatsGrid
        stats={createHealthStats(data)}
        heapMb={heapMb}
        limitMb={limitMb}
        memPercent={memPercent}
        pressureLevel={data?.pressure_level ?? 'Normal'}
      />
      <IndexHealthSummary />
    </div>
  );
}

function IndexesTab() {
  const { data: indexes, isLoading, isError } = useIndexes();

  if (isLoading) {
    return (
      <div className="space-y-2">
        {Array.from({ length: 5 }).map((_, i) => (
          <Skeleton key={i} className="h-12 w-full" />
        ))}
      </div>
    );
  }

  if (isError || !indexes) {
    return (
      <Card>
        <CardContent className="pt-6 text-center text-muted-foreground">
          Unable to load indexes.
        </CardContent>
      </Card>
    );
  }

  const interpretedIndexes = interpretIndexes(indexes);
  const totalDocs = interpretedIndexes.reduce((sum, { index }) => sum + (index.entries ?? 0), 0);
  const totalSize = interpretedIndexes.reduce((sum, { index }) => sum + (index.dataSize ?? 0), 0);
  const pendingTasks = interpretedIndexes.reduce((sum, { status }) => sum + status.pendingTasks, 0);

  return (
    <div className="space-y-4">
      <IndexesOverviewCards indexCount={indexes.length} totalDocs={totalDocs} totalSize={totalSize} />
      <StatusMeaningGuidance className="rounded-md border bg-muted/30 px-3 py-2 text-sm text-muted-foreground space-y-1" />
      <IndexesPendingTasksNotice pendingTasks={pendingTasks} />
      <IndexDetailsTable interpretedIndexes={interpretedIndexes} />
    </div>
  );
}

function ReplicationTab() {
  const { data, isLoading, isError, error } = useInternalStatus();

  if (isLoading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-24" />
        <Skeleton className="h-24" />
      </div>
    );
  }

  if (isError) {
    return (
      <Card>
        <CardContent className="pt-6">
          <div className="flex items-center gap-3 text-muted-foreground">
            <Server className="h-5 w-5" />
            <div>
              <p className="font-medium">Replication status unavailable</p>
              <p className="text-sm">{(error as Error)?.message || 'Could not reach internal status endpoint.'}</p>
            </div>
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="space-y-4">
      <p className="text-sm text-muted-foreground">Auto-refreshes every 10 seconds</p>
      <div className="grid gap-4 sm:grid-cols-2">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">Node ID</CardTitle>
            <Server className="h-5 w-5 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <p className="text-sm font-mono break-all" data-testid="node-id-value">{data?.node_id || 'N/A'}</p>
            {(!data?.node_id || data.node_id === 'unknown') && (
              <p className="text-xs text-muted-foreground mt-1">
                Expected for standalone instances. Node IDs are assigned when replication is configured.
              </p>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">Replication</CardTitle>
            {data?.replication_enabled ? (
              <CheckCircle className="h-5 w-5 text-green-600 dark:text-green-400" />
            ) : (
              <XCircle className="h-5 w-5 text-muted-foreground" />
            )}
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-bold" data-testid="replication-status">
              {data?.replication_enabled ? 'Enabled' : 'Disabled'}
            </p>
            {data?.replication_enabled && (
              <p className="text-sm text-muted-foreground mt-1">
                {data.peer_count} peer(s) connected
              </p>
            )}
          </CardContent>
        </Card>
      </div>

      {data?.ssl_renewal && (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">SSL / TLS</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2 text-sm">
            {data.ssl_renewal.certificate_expiry && (
              <p><span className="text-muted-foreground">Certificate expires:</span> {data.ssl_renewal.certificate_expiry}</p>
            )}
            {data.ssl_renewal.next_renewal && (
              <p><span className="text-muted-foreground">Next renewal:</span> {data.ssl_renewal.next_renewal}</p>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  );
}

function SnapshotsTab() {
  const { data: indexes, isLoading: indexesLoading } = useIndexes();
  const exportIndex = useExportIndex();
  const importIndex = useImportIndex();
  const snapshotToS3 = useSnapshotToS3();
  const restoreFromS3 = useRestoreFromS3();
  const fileInputRef = useRef<HTMLInputElement>(null);
  const importTargetRef = useRef<string>('');

  // Probe S3 availability by listing snapshots for the first index
  const firstIndex = indexes?.[0]?.uid || '';
  const { data: snapshots, isError: s3Error } = useListSnapshots(firstIndex);
  const s3Available = !s3Error && !!firstIndex;

  const handleImport = (indexName: string) => {
    importTargetRef.current = indexName;
    fileInputRef.current?.click();
  };

  const onFileSelected = (e: ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file && importTargetRef.current) {
      importIndex.mutate({ indexName: importTargetRef.current, file });
    }
    e.target.value = '';
  };

  const handleExportAll = () => {
    indexes?.forEach((idx) => exportIndex.mutate(idx.uid));
  };

  const handleBackupAll = () => {
    indexes?.forEach((idx) => snapshotToS3.mutate(idx.uid));
  };

  if (indexesLoading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-24" />
        <Skeleton className="h-24" />
      </div>
    );
  }

  if (!indexes || indexes.length === 0) {
    return (
      <Card>
        <CardContent className="pt-6 text-center text-muted-foreground">
          No indexes available. Create an index first to use snapshots.
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="space-y-6" data-testid="snapshots-tab">
      <input
        ref={fileInputRef}
        type="file"
        accept=".tar.gz,.tgz"
        className="hidden"
        onChange={onFileSelected}
        data-testid="snapshot-file-input"
      />
      <SnapshotsLocalCard
        indexes={indexes}
        exportPending={exportIndex.isPending}
        importPending={importIndex.isPending}
        onExportAll={handleExportAll}
        onExport={(indexName) => exportIndex.mutate(indexName)}
        onImport={handleImport}
      />
      <SnapshotsS3Card
        s3Available={s3Available}
        snapshots={snapshots}
        firstIndex={firstIndex}
        indexes={indexes}
        backupPending={snapshotToS3.isPending}
        restorePending={restoreFromS3.isPending}
        onBackupAll={handleBackupAll}
        onBackup={(indexName) => snapshotToS3.mutate(indexName)}
        onRestore={(indexName) => restoreFromS3.mutate(indexName)}
      />
    </div>
  );
}

export const System = memo(function System() {
  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <Activity className="h-6 w-6" />
        <h1 className="text-2xl font-bold">System</h1>
      </div>

      <Tabs defaultValue="health">
        <TabsList>
          <TabsTrigger value="health">Health</TabsTrigger>
          <TabsTrigger value="indexes">Indexes</TabsTrigger>
          <TabsTrigger value="replication">Replication</TabsTrigger>
          <TabsTrigger value="snapshots">Snapshots</TabsTrigger>
        </TabsList>

        <TabsContent value="health">
          <HealthTab />
        </TabsContent>

        <TabsContent value="indexes">
          <IndexesTab />
        </TabsContent>

        <TabsContent value="replication">
          <ReplicationTab />
        </TabsContent>

        <TabsContent value="snapshots">
          <SnapshotsTab />
        </TabsContent>
      </Tabs>
    </div>
  );
});
