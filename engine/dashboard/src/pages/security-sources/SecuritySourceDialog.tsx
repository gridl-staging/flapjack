import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Textarea } from '@/components/ui/textarea';

interface SecuritySourceDialogProps {
  open: boolean;
  isPending: boolean;
  sourceValue: string;
  descriptionValue: string;
  sourceValidationError: string;
  onOpenChange: (open: boolean) => void;
  onSourceChange: (value: string) => void;
  onDescriptionChange: (value: string) => void;
  onSubmit: () => Promise<void>;
}

export function SecuritySourceDialog({
  open,
  isPending,
  sourceValue,
  descriptionValue,
  sourceValidationError,
  onOpenChange,
  onSourceChange,
  onDescriptionChange,
  onSubmit,
}: SecuritySourceDialogProps) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Add Security Source</DialogTitle>
          <DialogDescription>
            Add an allowed CIDR range or IP address for secured API key requests.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="security-source-input">Source</Label>
            <Input
              id="security-source-input"
              value={sourceValue}
              onChange={(event) => onSourceChange(event.target.value)}
              placeholder="192.168.1.0/24"
            />
            {sourceValidationError && (
              <p className="text-sm text-destructive" role="alert">
                {sourceValidationError}
              </p>
            )}
          </div>

          <div className="space-y-2">
            <Label htmlFor="security-source-description">Description</Label>
            <Textarea
              id="security-source-description"
              value={descriptionValue}
              onChange={(event) => onDescriptionChange(event.target.value)}
              placeholder="office network"
              rows={3}
            />
          </div>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button onClick={onSubmit} disabled={isPending}>
            Add Source
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
