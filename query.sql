-- name: GetJob :one
SELECT * FROM jobs where completed_at IS NULL limit 1 for update skip locked;

-- name: FinishJob :one
UPDATE jobs SET completed_at = CURRENT_TIMESTAMP, completed_by = $2 WHERE id = $1 RETURNING *;

-- name: SeedJobs :exec
insert into jobs (name) values ('a'), ('b'), ('c'), ('d'), ('e'), ('f');

-- name: WipeJobs :exec
delete from jobs;
