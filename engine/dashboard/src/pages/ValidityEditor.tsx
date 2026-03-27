import { Button } from '@/components/ui/button';
import { Label } from '@/components/ui/label';
import type { TimeRange } from '@/lib/types';

interface ValidityEditorProps {
  validity: TimeRange[];
  onChange: (validity: TimeRange[]) => void;
}

function unixToDatetimeLocal(unix: number): string {
  if (!unix) return '';
  const d = new Date(unix * 1000);
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

function datetimeLocalToUnix(value: string): number {
  if (!value) return 0;
  return Math.floor(new Date(value).getTime() / 1000);
}

export function ValidityEditor({ validity, onChange }: ValidityEditorProps) {
  const addRange = () => {
    onChange([...validity, { from: 0, until: 0 }]);
  };

  const removeRange = (index: number) => {
    onChange(validity.filter((_, i) => i !== index));
  };

  const updateRange = (index: number, field: 'from' | 'until', value: string) => {
    onChange(
      validity.map((r, i) =>
        i === index ? { ...r, [field]: datetimeLocalToUnix(value) } : r,
      ),
    );
  };

  return (
    <div className="space-y-2 rounded-md border p-3">
      <div className="flex items-center justify-between">
        <p className="font-medium">Validity</p>
        <Button type="button" variant="outline" size="sm" onClick={addRange}>
          Add Time Range
        </Button>
      </div>
      {validity.length === 0 ? (
        <p className="text-sm text-muted-foreground">No time restrictions — rule is always active.</p>
      ) : (
        <div className="space-y-2">
          {validity.map((range, i) => {
            const n = i + 1;
            return (
              <div key={`validity-${i}`} className="flex items-end gap-2">
                <div className="flex-1 space-y-1">
                  <Label htmlFor={`validity-from-${i}`}>{`Valid From ${n}`}</Label>
                  <input
                    id={`validity-from-${i}`}
                    type="datetime-local"
                    className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                    value={unixToDatetimeLocal(range.from)}
                    onChange={(e) => updateRange(i, 'from', e.target.value)}
                  />
                </div>
                <div className="flex-1 space-y-1">
                  <Label htmlFor={`validity-until-${i}`}>{`Valid Until ${n}`}</Label>
                  <input
                    id={`validity-until-${i}`}
                    type="datetime-local"
                    className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                    value={unixToDatetimeLocal(range.until)}
                    onChange={(e) => updateRange(i, 'until', e.target.value)}
                  />
                </div>
                <Button type="button" variant="ghost" size="sm" onClick={() => removeRange(i)}>
                  {`Remove Time Range ${n}`}
                </Button>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
