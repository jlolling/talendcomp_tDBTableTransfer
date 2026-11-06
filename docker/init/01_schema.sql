-- test schema for the table transfer components
-- runs only when the database volume is created for the first time

create schema deploymentmanager;

set search_path to deploymentmanager;

-- source table used by PlayPostgresTableTransfer, covers the most common data types
create table job_tasks (
    task_id        bigint        not null primary key,
    job_name       varchar(128)  not null,
    description    text,
    priority       integer,
    amount         numeric(15,4),
    ratio          double precision,
    is_active      boolean,
    start_date     date,
    start_time     time,
    created_at     timestamp,
    created_at_tz  timestamptz,
    payload        bytea,
    attributes     jsonb
);

insert into job_tasks
select
    i,
    'job_' || (i % 500),
    case when i % 10 = 0 then null else 'Task number ' || i || ' with umlauts äöüß and ''quotes''' end,
    i % 7,
    round((i * 1.2345)::numeric, 4),
    i / 3.0,
    i % 2 = 0,
    date '2020-01-01' + (i % 2000),
    time '00:00:00' + (i % 86400) * interval '1 second',
    timestamp '2020-01-01 00:00:00' + i * interval '1 minute',
    timestamptz '2020-01-01 00:00:00+00' + i * interval '1 minute',
    case when i % 5 = 0 then null else decode(md5(i::text), 'hex') end,
    case when i % 4 = 0 then null else jsonb_build_object('id', i, 'tag', 'tag_' || (i % 13)) end
from generate_series(1, 100000) as i;

-- target table with the same structure
create table job_tasks_copy (like job_tasks including all);

-- target table which rejects some records to test the error handling (die on error on/off):
-- every record with priority = 6 violates the check constraint
create table job_tasks_reject (like job_tasks including all);
alter table job_tasks_reject add constraint job_tasks_reject_priority_check check (priority <> 6);

-- source table with duplicate keys to test failing batches because of the primary key in the target
create table job_tasks_duplicates as
select * from job_tasks where task_id <= 1000
union all
select * from job_tasks where task_id between 500 and 520;

-- small source table to check the transfer of the last (partial) batch
create table job_tasks_small as
select * from job_tasks where task_id <= 1234;

analyze;
