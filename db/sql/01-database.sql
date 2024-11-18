CREATE TABLE IF NOT EXISTS public.city (
    id SERIAL PRIMARY KEY,
    city_name TEXT
);

CREATE TABLE IF NOT EXISTS public.weather_data
(
    city text COLLATE pg_catalog."default",
    obs_time timestamp without time zone,
    temperature numeric,
    feels_like integer,
    pressure integer,
    humidity integer,
    dew_point numeric,
    uv_index numeric,
    cloud_coverage integer,
    visibility integer,
    wind_speed numeric,
    wind_direction integer,
    wind_gust numeric,
    batch_id uuid,
    status_code integer,
    temp_unit character(1) COLLATE pg_catalog."default",
    processed boolean DEFAULT false,
    file_type text COLLATE pg_catalog."default",
    iso_time text COLLATE pg_catalog."default"
);

INSERT INTO public.city (city_name)
SELECT 
    regexp_replace(name, '.*/', '') 
FROM
    pg_timezone_names
WHERE
    name ILIKE 'posix/Europe%'
    OR name ILIKE 'posix/Africa%'
    OR name ILIKE 'posix/Asia%'
    OR name ILIKE 'posix/America%';

CREATE FUNCTION public.kelvin_to_unit(temp_k numeric, unit character varying) RETURNS numeric
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF unit = 'F' THEN
        RETURN ((temp_k - 273.15) * 9/5) + 32;  -- Convert Kelvin to Celsius, then to Fahrenheit
    ELSE
        RETURN temp_k - 273.15;  -- Convert to Celsius
    END IF;
END;
$$;