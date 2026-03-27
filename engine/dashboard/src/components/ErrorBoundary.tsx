/**
 * @module Provides an ErrorBoundary component that catches render-time errors in its subtree and displays either a custom fallback or a default error panel with a retry option.
 */
import { Component, type ReactNode } from 'react';
import { Button } from '@/components/ui/button';
import { AlertTriangle } from 'lucide-react';

interface Props {
  children: ReactNode;
  fallback?: ReactNode;
}

interface State {
  hasError: boolean;
  error: Error | null;
}

/**
 * React class component that catches JavaScript errors in its child component tree and displays a fallback UI instead of crashing the entire app. Supports an optional custom fallback; otherwise renders a default error message with a retry button that resets the error state.
 */
export class ErrorBoundary extends Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error };
  }

  /**
   * Renders the fallback UI when an error has been caught, or the children when no error is present. If a custom `fallback` prop is provided it takes precedence over the default error display. The default UI shows the error message and a "Try again" button that clears the error state, re-attempting to render the children.
   * @returns The fallback UI or the wrapped children.
   */
  render() {
    if (this.state.hasError) {
      if (this.props.fallback) return this.props.fallback;

      return (
        <div className="flex flex-col items-center justify-center gap-4 py-16">
          <AlertTriangle className="h-10 w-10 text-destructive" />
          <h2 className="text-lg font-semibold">Something went wrong</h2>
          <p className="text-sm text-muted-foreground max-w-md text-center">
            {this.state.error?.message || 'An unexpected error occurred.'}
          </p>
          <Button
            variant="outline"
            onClick={() => this.setState({ hasError: false, error: null })}
          >
            Try again
          </Button>
        </div>
      );
    }

    return this.props.children;
  }
}
