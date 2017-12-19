CREATE OR REPLACE FUNCTION benchmark_test() RETURNS VOID AS $$
DECLARE
  v INTEGER; i INTEGER;
BEGIN
  for i in 1..1000 loop
    v := 1;
  end loop;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION benchmark(pCount INTEGER) RETURNS INTERVAL AS $$
DECLARE
  v INTEGER; vtTime TIMESTAMP;
BEGIN
  vtTime := clock_timestamp();

  FOR i IN 1..pCount LOOP
    PERFORM benchmark_test();
  END LOOP;

  RETURN clock_timestamp() - vtTime;
END;
$$ LANGUAGE plpgsql;
