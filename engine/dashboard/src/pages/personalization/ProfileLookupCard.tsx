import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import type { PersonalizationProfile } from '@/lib/types';

interface ProfileLookupCardProps {
  lookupInput: string;
  onLookupInputChange: (value: string) => void;
  onLookup: () => void;
  isLookupDisabled: boolean;
  hasLookupToken: boolean;
  isLoading: boolean;
  isError: boolean;
  profile: PersonalizationProfile | null | undefined;
}

function ProfileResults({ profile }: { profile: PersonalizationProfile }) {
  return (
    <div className="space-y-3">
      <p className="text-sm">
        <span className="font-medium">User token:</span> {profile.userToken}
      </p>
      {profile.lastEventAt && (
        <p className="text-sm">
          <span className="font-medium">Last event at:</span> {profile.lastEventAt}
        </p>
      )}

      <div className="space-y-2">
        {Object.entries(profile.scores).map(([facetName, values]) => (
          <div key={facetName} className="rounded-md border p-3 space-y-2">
            <p className="font-medium">{facetName}</p>
            <ul className="space-y-1">
              {Object.entries(values).map(([facetValue, score]) => (
                <li key={`${facetName}-${facetValue}`} className="text-sm">
                  {facetValue}: {score}
                </li>
              ))}
            </ul>
          </div>
        ))}
      </div>
    </div>
  );
}

export function ProfileLookupCard({
  lookupInput,
  onLookupInputChange,
  onLookup,
  isLookupDisabled,
  hasLookupToken,
  isLoading,
  isError,
  profile,
}: ProfileLookupCardProps) {
  return (
    <Card className="p-6 space-y-4">
      <h3 className="text-lg font-semibold">User profile lookup</h3>
      <div className="flex flex-col gap-3 md:flex-row">
        <Input
          data-testid="profile-lookup-input"
          placeholder="Enter user token"
          value={lookupInput}
          onChange={(event) => onLookupInputChange(event.target.value)}
        />
        <Button data-testid="profile-lookup-btn" onClick={onLookup} disabled={isLookupDisabled}>
          Lookup profile
        </Button>
      </div>

      {hasLookupToken && (
        <div data-testid="profile-results" className="space-y-3">
          {isLoading && <p className="text-sm text-muted-foreground">Loading profile...</p>}
          {isError && <p className="text-sm text-destructive">Failed to load profile.</p>}
          {!isLoading && !isError && profile === null && (
            <p className="text-sm text-muted-foreground">No profile found</p>
          )}
          {!isLoading && !isError && profile && <ProfileResults profile={profile} />}
        </div>
      )}
    </Card>
  );
}
