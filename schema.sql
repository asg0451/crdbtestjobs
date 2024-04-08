create table if not exists jobs (
  id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
  name text,
  created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  completed_at timestamp,
  completed_by text
);

-- index for pending jobs
create index if not exists pending_jobs_idx on jobs (id, name, created_at, completed_at) where completed_at is null;
