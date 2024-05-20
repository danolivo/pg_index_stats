-- Copied from stats_ext.sql
create function check_estimated_rows(text) returns table (estimated int, actual int)
language plpgsql as
$$
declare
    ln text;
    tmp text[];
    first_row bool := true;
begin
    for ln in
        execute format('explain analyze %s', $1)
    loop
        if first_row then
            first_row := false;
            tmp := regexp_match(ln, 'rows=(\d*) .* rows=(\d*)');
            return query select tmp[1]::int, tmp[2]::int;
        end if;
    end loop;
end;
$$;

-- test the extension working as a module only, without UI at all.
CREATE TABLE is_test(x1 integer, x2 integer, x3 integer, x4 integer);

-- unique value in each column, but highly correlated.
INSERT INTO is_test (x1,x2,x3,x4)
  SELECT x%10,x%10,x%10,x%10 FROM generate_series(1,1E3) AS x;
ANALYZE is_test;

SELECT check_estimated_rows('
  SELECT * FROM is_test WHERE x1=9 AND x2=9 AND x3=9 AND x4=9');

DROP TABLE is_test CASCADE;
