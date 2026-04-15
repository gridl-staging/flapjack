import { useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import {
  MigrationCredentialsCard,
  AlgoliaIndexPickerCard,
  MigrationErrorCard,
  MigrationHeader,
  MigrationIndexNamesCard,
  MigrationInfoCard,
  MigrationSubmitButton,
  MigrationSuccessCard,
} from './MigrateSections';
import {
  type AlgoliaIndexInfo,
  type MigrationResult,
  buildMigrationRequestBody,
  getIndexListErrorMessage,
  getMigrationErrorMessage,
  postSensitiveMigrationRequest,
  resolveEffectiveTargetIndex,
} from './migrateHelpers';

export function Migrate() {
  const queryClient = useQueryClient();

  const [appId, setAppId] = useState('');
  const [apiKey, setApiKey] = useState('');
  const [sourceIndex, setSourceIndex] = useState('');
  const [targetIndex, setTargetIndex] = useState('');
  const [overwrite, setOverwrite] = useState(false);
  const [showKey, setShowKey] = useState(false);

  // Algolia index listing
  const [algoliaIndexes, setAlgoliaIndexes] = useState<AlgoliaIndexInfo[] | null>(null);
  const [indexListError, setIndexListError] = useState<string | null>(null);
  const trimmedAppId = appId.trim();
  const trimmedApiKey = apiKey.trim();
  const trimmedSourceIndex = sourceIndex.trim();
  const trimmedTargetIndex = targetIndex.trim();
  const effectiveTarget = resolveEffectiveTargetIndex(trimmedSourceIndex, trimmedTargetIndex);

  const resetIndexListingState = () => {
    setAlgoliaIndexes(null);
    setIndexListError(null);
  };

  const fetchIndexes = useMutation({
    mutationFn: async () => {
      const data = await postSensitiveMigrationRequest<{ indexes: AlgoliaIndexInfo[] }>(
        '/1/algolia-list-indexes',
        { appId: trimmedAppId, apiKey: trimmedApiKey },
      );
      return data.indexes;
    },
    onSuccess: (indexes) => {
      setAlgoliaIndexes(indexes);
      setIndexListError(null);
      // Auto-select if there's only one index
      if (indexes.length === 1) {
        setSourceIndex(indexes[0].name);
      }
    },
    onError: (error) => {
      setAlgoliaIndexes(null);
      setIndexListError(getIndexListErrorMessage(error));
    },
  });

  const migration = useMutation({
    mutationFn: async () => {
      return postSensitiveMigrationRequest<MigrationResult>(
        '/1/migrate-from-algolia',
        buildMigrationRequestBody({
          appId: trimmedAppId,
          apiKey: trimmedApiKey,
          sourceIndex: trimmedSourceIndex,
          targetIndex: trimmedTargetIndex,
          overwrite,
        }),
      );
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['indexes'] });
    },
  });

  const hasCredentials = Boolean(trimmedAppId && trimmedApiKey);
  const canFetchIndexes = hasCredentials && !fetchIndexes.isPending;
  const canSubmit =
    Boolean(trimmedAppId && trimmedApiKey && trimmedSourceIndex) && !migration.isPending;

  return (
    <div className="space-y-6 max-w-2xl">
      <MigrationHeader effectiveTarget={effectiveTarget} />

      <MigrationCredentialsCard
        appId={appId}
        apiKey={apiKey}
        showKey={showKey}
        migrationPending={migration.isPending}
        hasCredentials={hasCredentials}
        canFetchIndexes={canFetchIndexes}
        fetchIndexesPending={fetchIndexes.isPending}
        algoliaIndexesLoaded={Boolean(algoliaIndexes)}
        onAppIdChange={(value) => {
          setAppId(value);
          resetIndexListingState();
        }}
        onApiKeyChange={(value) => {
          setApiKey(value);
          resetIndexListingState();
        }}
        onToggleShowKey={() => setShowKey(!showKey)}
        onFetchIndexes={() => fetchIndexes.mutate()}
      />

      <AlgoliaIndexPickerCard
        algoliaIndexes={algoliaIndexes}
        sourceIndex={sourceIndex}
        migrationPending={migration.isPending}
        indexListError={indexListError}
        onSelectSourceIndex={setSourceIndex}
      />

      <MigrationIndexNamesCard
        algoliaIndexesLoaded={Boolean(algoliaIndexes)}
        sourceIndex={sourceIndex}
        targetIndex={targetIndex}
        trimmedSourceIndex={trimmedSourceIndex}
        overwrite={overwrite}
        migrationPending={migration.isPending}
        onSourceIndexChange={setSourceIndex}
        onTargetIndexChange={setTargetIndex}
        onOverwriteChange={setOverwrite}
      />

      <MigrationSubmitButton
        canSubmit={canSubmit}
        migrationPending={migration.isPending}
        effectiveTarget={effectiveTarget}
        onSubmit={() => migration.mutate()}
      />

      {migration.isSuccess && migration.data && (
        <MigrationSuccessCard
          migrationData={migration.data}
          effectiveTarget={effectiveTarget}
        />
      )}

      {migration.isError && (
        <MigrationErrorCard errorMessage={getMigrationErrorMessage(migration.error)} />
      )}

      <MigrationInfoCard />
    </div>
  );
}
