-- FUNCTION: public.weather_data(text[], character, text)

-- DROP FUNCTION IF EXISTS public.weather_data(text[], character, text);

CREATE OR REPLACE FUNCTION public.weather_data(
	p_cities text[],
	p_temp_unit character,
	p_file_type text)
    RETURNS TABLE(batch_id uuid, status_code integer, file_type text, city text, temp_unit character, iso_time text, temperature numeric, feels_like numeric, pressure integer, humidity integer, dew_point numeric, uv_index numeric, cloud_coverage integer, visibility integer, wind_speed numeric, wind_direction integer, wind_gust numeric) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$

DECLARE
    -- Batch ID for this function call
    v_batch_id UUID := gen_random_uuid();
    v_status INTEGER DEFAULT 200;
    v_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP;

    -- Variables for city validation
    v_valid_cities TEXT[];
    v_city TEXT;
    v_valid_count INTEGER := 0;
    v_total_cities INTEGER := array_length(p_cities, 1);

    -- Base parameters for seasonal variations
    v_day_of_year DOUBLE PRECISION;
    v_hour_of_day DOUBLE PRECISION;
    v_seasonal_factor DOUBLE PRECISION;
    v_daytime_factor DOUBLE PRECISION;

    -- Weather parameters
    v_base_temp NUMERIC;
    v_temp NUMERIC;
    v_feels_like NUMERIC;
    v_pressure INTEGER;
    v_humidity INTEGER;
    v_dew_point NUMERIC;
    v_uvi NUMERIC;
    v_clouds INTEGER;
    v_visibility INTEGER;
    v_wind_speed NUMERIC;
    v_wind_deg INTEGER;
    v_wind_gust NUMERIC;

BEGIN

	IF UPPER(p_file_type) NOT IN ('CSV', 'JSON') THEN
	    v_status := 400; -- Bad request -- no or wrong file type 
	    RAISE NOTICE 'In IF file type %', p_file_type;
	    RETURN; -- Exit the function
	END IF;
    -- Set status code based on validation results
	IF UPPER(p_temp_unit) NOT IN ('C', 'F') THEN
        v_status := 400; -- Bad request -- no or wrong tempeature unit 
		RAISE NOTICE 'In IF temp unit %',p_temp_unit ;
		RETURN; -- Exit the function
	END IF;

	RAISE NOTICE 'This is total cities count %',v_total_cities;

    -- Get valid cities
	SELECT ARRAY_AGG(c.city_name)
	INTO v_valid_cities
	FROM public.city c
	WHERE LOWER(c.city_name) IN (
	    SELECT LOWER(city_name)
	    FROM unnest(p_cities) AS city_name
	);

	-- Count valid cities
    v_valid_count := COALESCE(array_length(v_valid_cities, 1), 0);

    IF v_valid_count = 0 THEN
		RAISE NOTICE 'In IF valid count %', v_valid_count;
		v_status := 400; -- Bad request - no valid cities
		RAISE NOTICE 'status %', v_status;
		RETURN; -- Exit the function
    ELSIF v_valid_count < v_total_cities THEN
        v_status := 206; -- Partial Content - some cities exist
		RAISE NOTICE 'In IF status %', v_status;
    ELSE
        v_status := 200; -- OK - all cities exist
		RAISE NOTICE 'In IF status %', v_status;
    END IF;

    -- Calculate day of year (0-365) and hour (0-24) factors
    v_day_of_year := EXTRACT(DOY FROM v_timestamp)::DOUBLE PRECISION;
    v_hour_of_day := EXTRACT(HOUR FROM v_timestamp)::DOUBLE PRECISION;

    -- Seasonal variation (-1 to 1, peaks in summer)
    v_seasonal_factor := SIN((v_day_of_year - 172) * 2 * pi() / 365);

    -- Daily variation (-1 to 1, peaks at 2 PM)
    v_daytime_factor := SIN((v_hour_of_day - 6) * pi() / 12);

    -- Process each valid city
	IF v_valid_count > 0 THEN
	    FOREACH v_city IN ARRAY v_valid_cities LOOP
	        -- Temperature calculations
	        v_base_temp := 15 + (15 * v_seasonal_factor);
	        v_temp := v_base_temp + (5 * v_daytime_factor) + (RANDOM() * 3 - 1.5) + 273.15;
	
	        -- Humidity
	        v_humidity := GREATEST(LEAST(
	            ROUND(65 - (20 * v_daytime_factor) - (10 * v_seasonal_factor) + 
	                  (RANDOM() * 20)),
	            100),
	            0);
	
	        -- Pressure varies seasonally and with temperature
	        v_pressure := 1013 + ROUND(
	            (-5 * v_seasonal_factor) + 
	            (-2 * v_daytime_factor) + 
	            (RANDOM() * 10)
	        );
	
	        -- UV Index varies by time of day and season
	        v_uvi := CASE 
	            WHEN v_hour_of_day BETWEEN 5 AND 19 THEN
	                GREATEST(
	                    (5 + (3 * v_seasonal_factor)) * 
	                    SIN((v_hour_of_day - 5) * pi() / 14) +
	                    (RANDOM() * 0.5),
	                    0)
	            ELSE 0
	        END;
	
	        -- Cloud coverage
	        v_clouds := GREATEST(LEAST(
	            ROUND(40 + (20 * v_seasonal_factor) + (RANDOM() * 40)),
	            100),
	            0);
	
	        -- Visibility
	        v_visibility := GREATEST(
	            ROUND(10000 - (v_humidity * 50) - (v_clouds * 30) + (RANDOM() * 1000)),
	            50);
	
	        -- Wind speed
	        v_wind_speed := GREATEST(
	            2 + (ABS(v_daytime_factor) * 3) + (RANDOM() * 5),
	            0);
	
	        -- Wind direction
	        v_wind_deg := ROUND(RANDOM() * 360);
	
	        -- Wind gusts
	        v_wind_gust := GREATEST(
	            v_wind_speed * (1.2 + (RANDOM() * 0.5)),
	            v_wind_speed);
	
	        -- Feels-like temperature
	        v_feels_like := CASE
	            WHEN v_temp < 283.15 THEN -- Wind chill
	                v_temp - (v_wind_speed * 0.2)
	            WHEN v_temp > 299.15 THEN -- Heat index
	                v_temp + (v_humidity * 0.1)
	            ELSE v_temp
	        END;
	
	        -- Dew point
	        v_dew_point := v_temp - ((100 - v_humidity) / 5);
	
	        -- Insert data for each city
	        INSERT INTO weather_data (
	            batch_id,
	            status_code,
				temp_unit,
				file_type,
	            city,
	            iso_time,
	            temperature,
	            feels_like,
	            pressure,
	            humidity,
	            dew_point,
	            uv_index,
	            cloud_coverage,
	            visibility,
	            wind_speed,
	            wind_direction,
	            wind_gust
	        )
	        VALUES (
	            v_batch_id::UUID,
	            v_status,
				UPPER(p_temp_unit)::CHARACTER,
				LOWER(p_file_type)::TEXT,
	            v_city,
	            TO_CHAR( v_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS'),
	            kelvin_to_unit(v_temp, p_temp_unit),
	            kelvin_to_unit(v_feels_like, p_temp_unit),
	            v_pressure,
	            v_humidity,
	            kelvin_to_unit(v_dew_point, p_temp_unit),
	            v_uvi,
	            v_clouds,
	            v_visibility,
	            v_wind_speed,
	            v_wind_deg,
	            v_wind_gust
	        );
	    END LOOP;
	END IF;
	
    -- Return all rows from table
        RETURN QUERY 
			SELECT 
			    wd.batch_id::UUID,         
			    wd.status_code::INTEGER,
			    wd.city::TEXT,
				wd.file_type::TEXT,
			    wd.temp_unit::CHARACTER,   
			    wd.iso_time::TEXT,
			    wd.temperature::NUMERIC,
			    wd.feels_like::NUMERIC,
			    wd.pressure::INTEGER,
			    wd.humidity::INTEGER,
			    wd.dew_point::NUMERIC,
			    wd.uv_index::NUMERIC,
			    wd.cloud_coverage::INTEGER,
			    wd.visibility::INTEGER,
			    wd.wind_speed::NUMERIC,
			    wd.wind_direction::INTEGER,
			    wd.wind_gust::NUMERIC
			FROM 
			    weather_data wd 
			WHERE 
			    wd.batch_id = v_batch_id;

END;
$BODY$;
