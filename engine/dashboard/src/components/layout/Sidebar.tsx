import { NavLink, useLocation } from 'react-router-dom';
import { X, Database, ChevronDown, ChevronRight } from 'lucide-react';
import { cn } from '@/lib/utils';
import { useEffect, useState } from 'react';
import { useIndexes } from '@/hooks/useIndexes';
import { InfoTooltip } from '@/components/ui/info-tooltip';
import { SidebarSection } from './SidebarSection';
import { MAX_VISIBLE_INDEXES } from './sidebar-index-constants';
import { SIDEBAR_SECTION_DEFINITIONS, type SidebarSectionDefinition } from './sidebar-nav';

interface SidebarProps {
  open?: boolean;
  onClose?: () => void;
}

const INDEX_SECTION_ID = 'indexes';

export function Sidebar({ open, onClose }: SidebarProps) {
  const location = useLocation();
  const { data: indexes } = useIndexes();
  const [showAllIndexes, setShowAllIndexes] = useState(false);
  const totalIndexesCount = indexes?.length ?? 0;

  // Close sidebar on route change (mobile)
  useEffect(() => {
    onClose?.();
  }, [location.pathname]); // intentionally omit onClose to avoid re-triggering on ref changes

  const visibleIndexes = showAllIndexes
    ? indexes
    : indexes?.slice(0, MAX_VISIBLE_INDEXES);

  const hasMoreIndexes = totalIndexesCount > MAX_VISIBLE_INDEXES;
  const hiddenIndexesCount = Math.max(0, totalIndexesCount - (visibleIndexes?.length ?? 0));
  const hiddenIndexesLabel = `${hiddenIndexesCount} more ${hiddenIndexesCount === 1 ? 'index' : 'indexes'}`;

  return (
    <>
      {/* Mobile overlay */}
      {open && (
        <div
          className="fixed inset-0 z-40 bg-black/50 md:hidden"
          onClick={onClose}
        />
      )}

      {/* Sidebar */}
      <aside
        className={cn(
          'border-r border-border bg-muted/20 p-4 z-50',
          // Desktop: always visible, static
          'hidden md:block w-64',
          // Mobile: slide-in overlay
          open && 'fixed inset-y-0 left-0 block w-64 md:relative md:inset-auto'
        )}
      >
        {/* Mobile close button */}
        <div className="flex items-center justify-between mb-4 md:hidden">
          <span className="text-sm font-semibold text-muted-foreground">Navigation</span>
          <button
            onClick={onClose}
            className="p-1 rounded-md hover:bg-accent"
            aria-label="Close navigation"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        {SIDEBAR_SECTION_DEFINITIONS.map((section) => (
          <SidebarSection
            key={section.id}
            sectionId={section.id}
            heading={renderSectionHeading(section)}
            headingLabel={section.heading}
            items={section.items}
            headingTestId={`sidebar-section-heading-${section.id}`}
            headingSuffix={section.id === INDEX_SECTION_ID ? (
              <InfoTooltip content="Each index is an isolated search collection with its own data, settings, and access controls." side="right" />
            ) : undefined}
            sectionTestId={`sidebar-section-${section.id}`}
          >
            {section.id === INDEX_SECTION_ID && indexes && indexes.length > 0 && (
              <div className="space-y-1" data-testid="sidebar-indexes">
                {visibleIndexes?.map((index) => {
                  const indexPath = `/index/${encodeURIComponent(index.uid)}`;
                  const isActive = isIndexPathActive(location.pathname, indexPath);
                  return (
                    <NavLink
                      key={index.uid}
                      to={indexPath}
                      className={cn(
                        'flex items-center gap-3 px-4 py-1.5 rounded-md text-sm transition-colors',
                        isActive
                          ? 'bg-primary/15 text-primary font-semibold'
                          : 'text-muted-foreground hover:bg-accent hover:text-accent-foreground',
                      )}
                      data-testid={`sidebar-index-${index.uid}`}
                    >
                      <Database className="h-4 w-4 shrink-0" />
                      <span className="truncate">{index.uid}</span>
                    </NavLink>
                  );
                })}
                {hasMoreIndexes && (
                  <button
                    onClick={() => setShowAllIndexes(!showAllIndexes)}
                    className="flex items-center gap-3 px-4 py-1.5 rounded-md text-xs text-muted-foreground hover:bg-accent hover:text-accent-foreground transition-colors w-full"
                    data-testid="sidebar-show-all-indexes"
                  >
                    {showAllIndexes ? (
                      <>
                        <ChevronDown className="h-3 w-3" />
                        Show less
                      </>
                    ) : (
                      <>
                        <ChevronRight className="h-3 w-3" />
                        Show all ({hiddenIndexesLabel})
                      </>
                    )}
                  </button>
                )}
              </div>
            )}
          </SidebarSection>
        ))}
      </aside>
    </>
  );
}

function renderSectionHeading(section: SidebarSectionDefinition) {
  if (section.id === INDEX_SECTION_ID) {
    return <span data-testid="sidebar-indexes-header">{section.heading}</span>;
  }
  return section.heading;
}

function isIndexPathActive(pathname: string, indexPath: string) {
  return pathname === indexPath || pathname.startsWith(`${indexPath}/`);
}
