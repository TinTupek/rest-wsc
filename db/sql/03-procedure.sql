-- FUNCTION: public.pull_weather_data()

-- DROP FUNCTION IF EXISTS public.pull_weather_data();

CREATE OR REPLACE FUNCTION public.pull_weather_data(
	)
    RETURNS TABLE(batch_id uuid, file_type text, city text, temp_unit character, iso_time text, temperature real, feels_like real, pressure integer, humidity integer, dew_point real, uv_index real, cloud_coverage integer, visibility integer, wind_speed real, wind_direction integer, wind_gust real) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE
    next_batch_id UUID;
BEGIN    
    -- Get the next unprocessed batch_id
    SELECT wd.batch_id INTO next_batch_id 
    FROM public.weather_data wd 
    WHERE wd.processed IS false 
    ORDER BY wd.obs_time 
    LIMIT 1;

    -- Mark the batch as processed (fixed the ambiguous reference)
    UPDATE public.weather_data wd
    SET processed = true
    WHERE wd.batch_id = next_batch_id;

    -- Return all rows from table
    RETURN QUERY 
        SELECT
            wd.batch_id::UUID,
            wd.file_type::TEXT,
            wd.city::TEXT,
            wd.temp_unit::CHARACTER,
            wd.iso_time::TEXT,
            wd.temperature::REAL,
            wd.feels_like::REAL,
            wd.pressure::INTEGER,
            wd.humidity::INTEGER,
            wd.dew_point::REAL,
            wd.uv_index::REAL,
            wd.cloud_coverage::INTEGER,
            wd.visibility::INTEGER,
            wd.wind_speed::REAL,
            wd.wind_direction::INTEGER,
            wd.wind_gust::REAL
        FROM 
            public.weather_data wd 
        WHERE
            wd.batch_id = next_batch_id;
END;
$BODY$;
