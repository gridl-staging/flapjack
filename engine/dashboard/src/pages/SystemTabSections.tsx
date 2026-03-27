import type { ReactNode } from 'react';
import { Link } from 'react-router-dom';
import type { LucideIcon } from 'lucide-react';
import {
  CheckCircle,
  Clock,
  Cloud,
  CloudOff,
  Cpu,
  Database,
  Download,
  HardDrive,
  Layers,
  RefreshCw,
  Upload,
  XCircle,
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { InfoTooltip } from '@/components/ui/info-tooltip';
import { useIndexes } from '@/hooks/useIndexes';
import type { S3Snapshot } from '@/hooks/useSnapshots';
import type { HealthDetail } from '@/hooks/useSystemStatus';
import { formatBytes, formatUptime } from '@/lib/utils';
import type { Index } from '@/lib/types';

type IndexStatusKey = 'healthy' | 'processing';

type HealthStat = {
  label: string;
  value: string;
  icon: LucideIcon;
  color: string;
  testId: string;
};

export type IndexStatusInterpretation = {
  key: IndexStatusKey;
  label: string;
  meaning: string;
  pendingTasks: number;
  dotClassName: string;
  dotPulseClassName: string;
  statusClassName: string;
  statusText: string;
};

export type InterpretedIndex = {
  index: Index;
  status: IndexStatusInterpretation;
};

const INDEX_STATUS_MEANING: Record<IndexStatusKey, { label: string; meaning: string }> = {
  healthy: {
    label: 'Healthy',
    meaning: 'Healthy indexes have no pending tasks.',
  },
  processing: {
    label: 'Processing',
    meaning: 'Processing indexes still have pending tasks in progress.',
  },
};

export function StatusMeaningGuidance({ className }: { className?: string }) {
  return (
    <div className={className ?? 'text-sm text-muted-foreground space-y-1'}>
      <p>{INDEX_STATUS_MEANING.healthy.meaning}</p>
      <p>{INDEX_STATUS_MEANING.processing.meaning}</p>
    </div>
  );
}

export function interpretIndexStatus(numberOfPendingTasks?: number): IndexStatusInterpretation {
  const pendingTasks = numberOfPendingTasks ?? 0;
  if (pendingTasks === 0) {
    return {
      key: 'healthy',
      label: INDEX_STATUS_MEANING.healthy.label,
      meaning: INDEX_STATUS_MEANING.healthy.meaning,
      pendingTasks,
      dotClassName: 'bg-green-500',
      dotPulseClassName: '',
      statusClassName: 'text-green-600 dark:text-green-400',
      statusText: 'Healthy (no pending tasks)',
    };
  }

  const pendingTaskLabel = pendingTasks === 1 ? 'task' : 'tasks';
  return {
    key: 'processing',
    label: INDEX_STATUS_MEANING.processing.label,
    meaning: INDEX_STATUS_MEANING.processing.meaning,
    pendingTasks,
    dotClassName: 'bg-amber-500',
    dotPulseClassName: 'animate-pulse',
    statusClassName: 'text-amber-600 dark:text-amber-400',
    statusText: `Processing (${pendingTasks} pending ${pendingTaskLabel})`,
  };
}

function buildIndexPath(indexUid: string) {
  return `/index/${encodeURIComponent(indexUid)}`;
}

export function interpretIndexes(indexes: Index[]): InterpretedIndex[] {
  return indexes.map((index) => ({
    index,
    status: interpretIndexStatus(index.numberOfPendingTasks ?? undefined),
  }));
}

export function createHealthStats(data?: HealthDetail): HealthStat[] {
  const isHealthy = data?.status === 'ok';
  return [
    {
      label: 'Status',
      value: data?.status || 'unknown',
      icon: isHealthy ? CheckCircle : XCircle,
      color: isHealthy ? 'text-green-600 dark:text-green-400' : 'text-destructive',
      testId: 'health-status',
    },
    {
      label: 'Active Writers',
      value: `${data?.active_writers ?? 0} / ${data?.max_concurrent_writers ?? 0}`,
      icon: Database,
      color: 'text-blue-600 dark:text-blue-400',
      testId: 'health-active-writers',
    },
    {
      label: 'Facet Cache',
      value: `${data?.facet_cache_entries ?? 0} / ${data?.facet_cache_cap ?? 0}`,
      icon: Layers,
      color: 'text-purple-600 dark:text-purple-400',
      testId: 'health-facet-cache',
    },
    {
      label: 'Uptime',
      value: formatUptime(data?.uptime_secs ?? 0),
      icon: Clock,
      color: 'text-emerald-600 dark:text-emerald-400',
      testId: 'health-uptime',
    },
    {
      label: 'Tenants Loaded',
      value: String(data?.tenants_loaded ?? 0),
      icon: Database,
      color: 'text-indigo-600 dark:text-indigo-400',
      testId: 'health-tenants-loaded',
    },
  ];
}

export function IndexHealthSummary() {
  const { data: indexes, isLoading } = useIndexes();

  if (isLoading || !indexes || indexes.length === 0) return null;

  const interpretedIndexes = interpretIndexes(indexes);
  const healthyCount = interpretedIndexes.filter(({ status }) => status.key === 'healthy').length;
  const totalPending = interpretedIndexes.reduce((sum, { status }) => sum + status.pendingTasks, 0);

  return (
    <Card data-testid="index-health-summary">
      <CardHeader className="pb-2">
        <CardTitle className="text-base flex items-center gap-1.5">
          Index Health
          <InfoTooltip content="Shows index task state. Healthy means no pending tasks. Processing means pending tasks are still in progress." />
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="flex flex-wrap items-center gap-3 mb-2">
          {interpretedIndexes.map(({ index, status }) => (
            <Link
              key={index.uid}
              to={buildIndexPath(index.uid)}
              className="flex items-center gap-1.5 hover:bg-accent/50 rounded-md px-1.5 py-0.5 transition-colors"
              data-testid={`index-dot-${index.uid}`}
            >
              <span
                className={`inline-block h-2.5 w-2.5 rounded-full ${status.dotClassName} ${status.dotPulseClassName}`}
              />
              <span className="text-sm">{index.uid}</span>
              <span className={`text-xs ${status.statusClassName}`}>{status.label}</span>
            </Link>
          ))}
        </div>
        <p className="text-sm text-muted-foreground">
          {healthyCount} of {indexes.length} indexes healthy{totalPending > 0 ? ` · ${totalPending} pending task(s)` : ''}
        </p>
        <StatusMeaningGuidance className="mt-2 text-xs text-muted-foreground space-y-1" />
      </CardContent>
    </Card>
  );
}

export function PressureDot({ level }: { level: string }) {
  const normalized = level.charAt(0).toUpperCase() + level.slice(1).toLowerCase();
  const colorClass =
    normalized === 'Critical'
      ? 'bg-red-500'
      : normalized === 'Elevated'
        ? 'bg-amber-500'
        : 'bg-green-500';
  return (
    <span className="inline-flex items-center gap-1.5" data-testid="health-pressure">
      <span className={`inline-block h-2.5 w-2.5 rounded-full ${colorClass}`} />
      <span className="text-sm">{normalized}</span>
    </span>
  );
}

function HealthStatCard({ stat }: { stat: HealthStat }) {
  return (
    <Card key={stat.testId} data-testid={stat.testId}>
      <CardHeader className="flex flex-row items-center justify-between pb-2">
        <CardTitle className="text-sm font-medium text-muted-foreground">{stat.label}</CardTitle>
        <stat.icon className={`h-5 w-5 ${stat.color}`} />
      </CardHeader>
      <CardContent>
        <p className="text-2xl font-bold" data-testid="stat-value">{stat.value}</p>
      </CardContent>
    </Card>
  );
}

type HealthStatsGridProps = {
  stats: HealthStat[];
  heapMb: number;
  limitMb: number;
  memPercent: number;
  pressureLevel: string;
};

export function HealthStatsGrid({
  stats,
  heapMb,
  limitMb,
  memPercent,
  pressureLevel,
}: HealthStatsGridProps) {
  return (
    <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
      {stats.map((stat) => (
        <HealthStatCard key={stat.testId} stat={stat} />
      ))}
      <Card data-testid="health-memory">
        <CardHeader className="flex flex-row items-center justify-between pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">Memory</CardTitle>
          <Cpu className="h-5 w-5 text-orange-600 dark:text-orange-400" />
        </CardHeader>
        <CardContent className="space-y-2">
          <p className="text-2xl font-bold" data-testid="stat-value">
            {heapMb} MB / {limitMb} MB ({memPercent}%)
          </p>
          <div className="h-2 w-full rounded-full bg-muted">
            <div
              className="h-2 rounded-full bg-orange-500 transition-all"
              style={{ width: `${Math.min(memPercent, 100)}%` }}
            />
          </div>
          <PressureDot level={pressureLevel} />
        </CardContent>
      </Card>
    </div>
  );
}

function OverviewCard({ label, value, testId }: { label: string; value: string; testId: string }) {
  return (
    <Card data-testid={testId}>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-medium text-muted-foreground">{label}</CardTitle>
      </CardHeader>
      <CardContent>
        <p className="text-2xl font-bold" data-testid="stat-value">{value}</p>
      </CardContent>
    </Card>
  );
}

type IndexesOverviewCardsProps = {
  indexCount: number;
  totalDocs: number;
  totalSize: number;
};

export function IndexesOverviewCards({ indexCount, totalDocs, totalSize }: IndexesOverviewCardsProps) {
  const cards = [
    { label: 'Total Indexes', value: String(indexCount), testId: 'indexes-total-count' },
    { label: 'Total Documents', value: totalDocs.toLocaleString(), testId: 'indexes-total-docs' },
    { label: 'Total Storage', value: formatBytes(totalSize), testId: 'indexes-total-storage' },
  ];

  return (
    <div className="grid gap-4 sm:grid-cols-3">
      {cards.map((card) => (
        <OverviewCard key={card.testId} {...card} />
      ))}
    </div>
  );
}

export function IndexesPendingTasksNotice({ pendingTasks }: { pendingTasks: number }) {
  if (pendingTasks === 0) return null;

  return (
    <Card>
      <CardContent className="pt-6">
        <div className="flex items-center gap-2 text-amber-600 dark:text-amber-400">
          <RefreshCw className="h-4 w-4 animate-spin" />
          <span className="text-sm font-medium">{pendingTasks} pending task(s) across indexes</span>
        </div>
      </CardContent>
    </Card>
  );
}

function IndexDetailsRow({ index, status }: InterpretedIndex) {
  const StatusIcon = status.key === 'healthy' ? CheckCircle : RefreshCw;

  return (
    <tr key={index.uid} className="border-b last:border-0">
      <td className="py-2 pr-4 font-medium">
        <Link
          to={buildIndexPath(index.uid)}
          className="text-primary hover:underline"
          data-testid={`index-link-${index.uid}`}
        >
          {index.uid}
        </Link>
      </td>
      <td className="py-2 pr-4" data-testid={`index-status-${index.uid}`}>
        <span className={`inline-flex items-center gap-1 ${status.statusClassName}`}>
          <StatusIcon className={`h-4 w-4 ${status.key === 'processing' ? 'animate-spin' : ''}`} />
          {status.statusText}
        </span>
      </td>
      <td className="py-2 pr-4 text-right" data-testid={`index-doc-count-${index.uid}`}>
        {(index.entries ?? 0).toLocaleString()}
      </td>
      <td className="py-2 pr-4 text-right" data-testid={`index-storage-${index.uid}`}>
        {formatBytes(index.dataSize ?? 0)}
      </td>
      <td className="py-2 text-right">
        {status.pendingTasks > 0 ? (
          <span className="text-amber-600 dark:text-amber-400">{status.pendingTasks}</span>
        ) : (
          <span className="text-muted-foreground">0</span>
        )}
      </td>
    </tr>
  );
}

export function IndexDetailsTable({ interpretedIndexes }: { interpretedIndexes: InterpretedIndex[] }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base flex items-center gap-1.5">
          Index Details
          <InfoTooltip content="Each index is an isolated search collection with its own data, settings, and access controls." />
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-left text-muted-foreground">
                <th className="pb-2 pr-4 font-medium">Name</th>
                <th className="pb-2 pr-4 font-medium">Status</th>
                <th className="pb-2 pr-4 font-medium text-right">Documents</th>
                <th className="pb-2 pr-4 font-medium text-right">Size</th>
                <th className="pb-2 font-medium text-right">Pending</th>
              </tr>
            </thead>
            <tbody>
              {interpretedIndexes.map(({ index, status }) => (
                <IndexDetailsRow key={index.uid} index={index} status={status} />
              ))}
            </tbody>
          </table>
        </div>
      </CardContent>
    </Card>
  );
}

type SnapshotIndexRowProps = {
  index: Index;
  testId: string;
  children: ReactNode;
};

function SnapshotIndexRow({ index, testId, children }: SnapshotIndexRowProps) {
  return (
    <div
      className="flex items-center justify-between p-3 rounded-md border border-border"
      data-testid={testId}
    >
      <div>
        <span className="font-medium text-sm">{index.uid}</span>
        <span className="text-xs text-muted-foreground ml-2">
          {(index.entries ?? 0).toLocaleString()} docs · {formatBytes(index.dataSize ?? 0)}
        </span>
      </div>
      <div className="flex gap-2">{children}</div>
    </div>
  );
}

type SnapshotsLocalCardProps = {
  indexes: Index[];
  exportPending: boolean;
  importPending: boolean;
  onExportAll: () => void;
  onExport: (indexName: string) => void;
  onImport: (indexName: string) => void;
};

export function SnapshotsLocalCard({
  indexes,
  exportPending,
  importPending,
  onExportAll,
  onExport,
  onImport,
}: SnapshotsLocalCardProps) {
  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle className="text-base flex items-center gap-2">
            <HardDrive className="h-4 w-4" />
            Local Export / Import
            <InfoTooltip content="Export an index as a tar.gz file to your local machine, or import a tar.gz file into an existing index." />
          </CardTitle>
          <Button
            variant="outline"
            size="sm"
            onClick={onExportAll}
            disabled={exportPending}
            data-testid="export-all-btn"
          >
            <Download className="h-4 w-4 mr-1" />
            Export All
          </Button>
        </div>
      </CardHeader>
      <CardContent>
        <div className="space-y-2">
          {indexes.map((index) => (
            <SnapshotIndexRow key={index.uid} index={index} testId={`snapshot-index-${index.uid}`}>
              <Button
                variant="outline"
                size="sm"
                onClick={() => onExport(index.uid)}
                disabled={exportPending}
                data-testid={`export-btn-${index.uid}`}
              >
                <Download className="h-3 w-3 mr-1" />
                Export
              </Button>
              <Button
                variant="outline"
                size="sm"
                onClick={() => onImport(index.uid)}
                disabled={importPending}
                data-testid={`import-btn-${index.uid}`}
              >
                <Upload className="h-3 w-3 mr-1" />
                Import
              </Button>
            </SnapshotIndexRow>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}

type SnapshotsS3CardProps = {
  s3Available: boolean;
  snapshots?: S3Snapshot[];
  firstIndex: string;
  indexes: Index[];
  backupPending: boolean;
  restorePending: boolean;
  onBackupAll: () => void;
  onBackup: (indexName: string) => void;
  onRestore: (indexName: string) => void;
};

export function SnapshotsS3Card({
  s3Available,
  snapshots,
  firstIndex,
  indexes,
  backupPending,
  restorePending,
  onBackupAll,
  onBackup,
  onRestore,
}: SnapshotsS3CardProps) {
  return (
    <Card data-testid="s3-section">
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle className="text-base flex items-center gap-2">
            {s3Available ? (
              <Cloud className="h-4 w-4 text-blue-500" />
            ) : (
              <CloudOff className="h-4 w-4 text-muted-foreground" />
            )}
            S3 Backups
            <InfoTooltip content="Back up indexes to S3-compatible storage. Requires FLAPJACK_S3_BUCKET and FLAPJACK_S3_REGION environment variables." />
          </CardTitle>
          {s3Available && (
            <Button
              variant="outline"
              size="sm"
              onClick={onBackupAll}
              disabled={backupPending}
              data-testid="backup-all-s3-btn"
            >
              <Cloud className="h-4 w-4 mr-1" />
              Backup All to S3
            </Button>
          )}
        </div>
      </CardHeader>
      <CardContent>
        {!s3Available ? (
          <div className="text-sm text-muted-foreground space-y-2" data-testid="s3-not-configured">
            <p>S3 backups are not configured. To enable, set these environment variables:</p>
            <code className="block bg-muted px-3 py-2 rounded text-xs">
              FLAPJACK_S3_BUCKET=your-bucket-name<br />
              FLAPJACK_S3_REGION=us-east-1<br />
              FLAPJACK_S3_ENDPOINT=https://s3.amazonaws.com  (optional)
            </code>
          </div>
        ) : (
          <div className="space-y-3">
            {snapshots && snapshots.length > 0 && (
              <div className="text-sm text-muted-foreground mb-2">
                {snapshots.length} snapshot(s) available for {firstIndex}
              </div>
            )}
            {indexes.map((index) => (
              <SnapshotIndexRow key={index.uid} index={index} testId={`s3-index-${index.uid}`}>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => onBackup(index.uid)}
                  disabled={backupPending}
                  data-testid={`backup-btn-${index.uid}`}
                >
                  <Cloud className="h-3 w-3 mr-1" />
                  Backup
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => onRestore(index.uid)}
                  disabled={restorePending}
                  data-testid={`restore-btn-${index.uid}`}
                >
                  <RefreshCw className="h-3 w-3 mr-1" />
                  Restore
                </Button>
              </SnapshotIndexRow>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
