export type FailurePhase = string;

export type Span = {
  name: string;
  kind: 'CLIENT' | 'INTERNAL';
  status: 'ERROR' | 'UNSET';
  start: number;
  duration: number;
  depth: number;
};

export type ErrorEvent = {
  name: string;
  phase: string;
  source: string;
  code: string;
  decision: string;
  attributes?: Record<string, string>;
};

export type ConnectionFailure = {
  id: string;
  time: string;
  phase: FailurePhase;
  category: string;
  errorType: string;
  duration: number;
  attempts: number;
  source: string;
  region: string;
  driver: string;
  attributes: Record<string, string>;
  spans: Span[];
  events: ErrorEvent[];
};

export type StatementFailure = {
  id: string;
  time: string;
  statementType: string;
  operation: string;
  maskedSql: string;
  category: string;
  errorType: string;
  duration: number;
  connectionId: string;
  attributes: Record<string, string>;
  spans: Span[];
  events: ErrorEvent[];
};

export type PerformanceMetric = {
  activity: string;
  label: string;
  kind: 'Connection' | 'Statement';
  count: number | null;
  successRate: number | null;
  errors: number | null;
  p50: number | null;
  p95: number | null;
  p99: number | null;
  max: number | null;
  totalDurationMs?: number | null;
  meanDurationMs?: number | null;
};

export type OutcomeDistribution = {
  activity: string;
  success: number | null;
  failure: number | null;
  timeout: number | null;
  canceled: number | null;
};

export type RetryMetric = { activity: string; reason: string; count: number | null };

export type ThroughputPoint = {
  label: string;
  connections: number | null;
  statements: number | null;
};