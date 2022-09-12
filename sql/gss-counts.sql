SELECT 'insert into public.check_counts (select now(), ''' || schemaname || ''''
|| ' (select type, count(*) from ' || schemaname || '.element group by type);'
FROM pg_catalog.pg_tables
WHERE schemaname != 'pg_catalog' AND
        schemaname != 'information_schema'
AND schemaname like 'wld%' and tableowner = 'gss_matter_owner_user'
and tablename ='element'
;

SELECT count(*)
FROM pg_catalog.pg_tables
WHERE schemaname != 'pg_catalog' AND
        schemaname != 'information_schema'
  AND schemaname like 'wld%' and tableowner = 'gss_matter_owner_user'
  and tablename ='element'
;

select type, count(*) from wld_161.element group by type limit 1000;
select type, count(*) from wld_98.element group by type


select *
FROM pg_catalog.pg_tables
WHERE schemaname != 'pg_catalog' AND
        schemaname != 'information_schema'
  AND schemaname like 'wld%' and tableowner = 'gss_matter_owner_user'
  and tablename ='element'
;

select check_counts.type, SUM(check_counts.count) from public.check_counts group by check_counts.type;
select * from public.check_counts limit 1;
