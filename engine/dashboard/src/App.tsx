/**
 * @module Application entry point that sets up authentication, theming, toast notifications, and code-split routing for all dashboard pages.
 */
import { lazy, Suspense } from 'react';
import { Routes, Route } from 'react-router-dom';
import { useTheme } from './hooks/useTheme';
import { Layout } from './components/layout/Layout';
import { IndexLayout } from './components/layout/IndexLayout';
import { AuthGate } from './components/layout/AuthGate';
import { ErrorBoundary } from './components/ErrorBoundary';
import { Toaster } from './components/ui/toaster';

// Lazy-load all route pages to keep initial bundle small.
// Each page (+ its deps like recharts) loads on demand.
const Overview = lazy(() => import('./pages/Overview').then(m => ({ default: m.Overview })));
const SearchBrowse = lazy(() => import('./pages/SearchBrowse').then(m => ({ default: m.SearchBrowse })));
const Settings = lazy(() => import('./pages/Settings').then(m => ({ default: m.Settings })));
const Analytics = lazy(() => import('./pages/Analytics').then(m => ({ default: m.Analytics })));
const Synonyms = lazy(() => import('./pages/Synonyms').then(m => ({ default: m.Synonyms })));
const Rules = lazy(() => import('./pages/Rules').then(m => ({ default: m.Rules })));
const MerchandisingStudio = lazy(() => import('./pages/MerchandisingStudio').then(m => ({ default: m.MerchandisingStudio })));
const Recommendations = lazy(() => import('./pages/Recommendations').then(m => ({ default: m.Recommendations })));
const Chat = lazy(() => import('./pages/Chat').then(m => ({ default: m.Chat })));
const ApiKeys = lazy(() => import('./pages/ApiKeys').then(m => ({ default: m.ApiKeys })));
const SearchLogs = lazy(() => import('./pages/SearchLogs').then(m => ({ default: m.SearchLogs })));
const System = lazy(() => import('./pages/System').then(m => ({ default: m.System })));
const Metrics = lazy(() => import('./pages/Metrics').then(m => ({ default: m.Metrics })));
const Cluster = lazy(() => import('./pages/Cluster').then(m => ({ default: m.Cluster })));
const Migrate = lazy(() => import('./pages/Migrate').then(m => ({ default: m.Migrate })));
const QuerySuggestions = lazy(() => import('./pages/QuerySuggestions').then(m => ({ default: m.QuerySuggestions })));
const Experiments = lazy(() => import('./pages/Experiments').then(m => ({ default: m.Experiments })));
const ExperimentDetail = lazy(() => import('./pages/ExperimentDetail').then(m => ({ default: m.ExperimentDetail })));
const EventDebugger = lazy(() => import('./pages/EventDebugger').then(m => ({ default: m.EventDebugger })));
const Personalization = lazy(() => import('./pages/Personalization').then(m => ({ default: m.Personalization })));
const Dictionaries = lazy(() => import('./pages/Dictionaries').then(m => ({ default: m.Dictionaries })));
const SecuritySources = lazy(() => import('./pages/SecuritySources').then(m => ({ default: m.SecuritySources })));

function LazyPage({ children }: { children: React.ReactNode }) {
  return (
    <ErrorBoundary>
      <Suspense fallback={<div className="p-6 animate-pulse">Loading...</div>}>
        {children}
      </Suspense>
    </ErrorBoundary>
  );
}

function renderLazyPage(page: React.ReactNode) {
  return <LazyPage>{page}</LazyPage>;
}

/**
 * Root application component that initializes theming, enforces authentication via AuthGate, and renders the top-level router. All page routes are code-split and wrapped in Suspense with an error boundary so heavy dependencies (e.g. recharts) load on demand.
 */
function App() {
  // Initialize theme
  useTheme();

  return (
    <AuthGate>
      <Routes>
        <Route path="/" element={<Layout />}>
          <Route index element={renderLazyPage(<Overview />)} />
          <Route path="overview" element={renderLazyPage(<Overview />)} />
          <Route path="index/:indexName" element={<IndexLayout />}>
            <Route index element={renderLazyPage(<SearchBrowse />)} />
            <Route path="settings" element={renderLazyPage(<Settings />)} />
            <Route path="analytics" element={renderLazyPage(<Analytics />)} />
            <Route path="synonyms" element={renderLazyPage(<Synonyms />)} />
            <Route path="rules" element={renderLazyPage(<Rules />)} />
            <Route path="merchandising" element={renderLazyPage(<MerchandisingStudio />)} />
            <Route path="recommendations" element={renderLazyPage(<Recommendations />)} />
            <Route path="chat" element={renderLazyPage(<Chat />)} />
          </Route>
          <Route path="keys" element={renderLazyPage(<ApiKeys />)} />
          <Route path="logs" element={renderLazyPage(<SearchLogs />)} />
          <Route path="migrate" element={renderLazyPage(<Migrate />)} />
          <Route path="metrics" element={renderLazyPage(<Metrics />)} />
          <Route path="cluster" element={renderLazyPage(<Cluster />)} />
          <Route path="system" element={renderLazyPage(<System />)} />
          <Route path="query-suggestions" element={renderLazyPage(<QuerySuggestions />)} />
          <Route path="experiments" element={renderLazyPage(<Experiments />)} />
          <Route path="experiments/:experimentId" element={renderLazyPage(<ExperimentDetail />)} />
          <Route path="events" element={renderLazyPage(<EventDebugger />)} />
          <Route path="personalization" element={renderLazyPage(<Personalization />)} />
          <Route path="dictionaries" element={renderLazyPage(<Dictionaries />)} />
          <Route path="security-sources" element={renderLazyPage(<SecuritySources />)} />
          <Route path="*" element={<div className="p-6">Page not found</div>} />
        </Route>
      </Routes>
      <Toaster />
    </AuthGate>
  );
}

export default App;
