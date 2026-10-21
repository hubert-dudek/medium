import { useMemo, useState } from 'react';
import {
  Badge,
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from '@databricks/appkit-ui/react';
import { Newspaper } from 'lucide-react';

type Topic = 'Apps' | 'AI/ML' | 'Data Engineering';

interface NewsItem {
  id: number;
  topic: Topic;
  title: string;
  summary: string;
}

const NEWS: NewsItem[] = [
  {
    id: 1,
    topic: 'Apps',
    title: 'Deployment history',
    summary: 'A demo card showing how past app deployments might appear in a simple timeline.',
  },
  {
    id: 2,
    topic: 'Apps',
    title: 'App Builder',
    summary: 'A demo card illustrating how an app could be assembled from reusable building blocks.',
  },
  {
    id: 3,
    topic: 'AI/ML',
    title: 'Genie CLI',
    summary: 'A demo card imagining a command-line way to ask questions about sample data.',
  },
  {
    id: 4,
    topic: 'AI/ML',
    title: 'Agent memory',
    summary: 'A demo card sketching how an assistant might recall earlier turns in a conversation.',
  },
  {
    id: 5,
    topic: 'Data Engineering',
    title: 'Materialized views',
    summary: 'A demo card describing how precomputed results could speed up a sample dashboard.',
  },
  {
    id: 6,
    topic: 'Data Engineering',
    title: 'JDBC connections',
    summary: 'A demo card outlining how an external tool might connect to a mock data source.',
  },
];

const TOPICS: Topic[] = ['Apps', 'AI/ML', 'Data Engineering'];

const ALL = 'All' as const;
type Filter = typeof ALL | Topic;

const FILTERS: Filter[] = [ALL, ...TOPICS];

function FilterPill({
  label,
  active,
  onClick,
}: {
  label: string;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`rounded-full px-4 py-1.5 text-sm font-medium transition-colors border ${
        active
          ? 'bg-primary text-primary-foreground border-primary'
          : 'bg-background text-foreground border-border hover:border-primary hover:text-primary'
      }`}
    >
      {label}
    </button>
  );
}

export default function App() {
  const [filter, setFilter] = useState<Filter>(ALL);

  const items = useMemo(
    () => (filter === ALL ? NEWS : NEWS.filter((n) => n.topic === filter)),
    [filter],
  );

  return (
    <div className="min-h-screen bg-background text-foreground">
      <header className="border-b bg-background">
        <div className="mx-auto max-w-5xl px-4 md:px-6 py-6 flex items-center gap-3">
          <span className="flex h-10 w-10 items-center justify-center rounded-lg bg-primary text-primary-foreground">
            <Newspaper className="h-5 w-5" />
          </span>
          <div>
            <h1 className="text-xl font-bold tracking-tight">Databricks News Tracker</h1>
            <p className="text-sm text-muted-foreground">Sample news for a demo app</p>
          </div>
        </div>
      </header>

      <main className="mx-auto max-w-5xl px-4 md:px-6 py-8 space-y-6">
        <div className="flex flex-wrap items-center gap-2">
          <span className="text-sm font-medium text-muted-foreground mr-1">Topic:</span>
          {FILTERS.map((f) => (
            <FilterPill key={f} label={f} active={filter === f} onClick={() => setFilter(f)} />
          ))}
        </div>

        <p className="text-sm text-muted-foreground">
          Showing {items.length} {items.length === 1 ? 'story' : 'stories'}
          {filter !== ALL ? ` in ${filter}` : ''}
        </p>

        {items.length === 0 ? (
          <Card>
            <CardContent className="py-12 text-center text-muted-foreground">
              No stories for this topic yet.
            </CardContent>
          </Card>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-5">
            {items.map((item) => (
              <Card
                key={item.id}
                className="flex flex-col border-t-4 border-t-primary transition-shadow hover:shadow-lg"
              >
                <CardHeader className="space-y-3">
                  <Badge className="w-fit bg-primary text-primary-foreground hover:bg-primary">
                    {item.topic}
                  </Badge>
                  <CardTitle className="text-base leading-snug">{item.title}</CardTitle>
                </CardHeader>
                <CardContent className="flex-1">
                  <p className="text-sm text-muted-foreground">{item.summary}</p>
                </CardContent>
              </Card>
            ))}
          </div>
        )}
      </main>
    </div>
  );
}
