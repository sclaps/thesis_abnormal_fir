WITH flight_scope AS (
    SELECT
        FL.FlightLegUtcId                                               AS flight_key_id,
        FL.ScheduledArrivalTimeUtc                                      AS fl_scheduled_arrival_time_utc,
        FL.ActualArrivalTimeUtc                                         AS fl_actual_arrival_time_utc,
        FL.DepartureAirport                                             AS fl_departure_airport,
        FL.ArrivalAirport                                               AS fl_arrival_airport,
        LKPA.IcaoAirportCode                                            AS lkpa_arrival_airport_icao_code
    FROM `afkl-dakota-p300.ds_kl_sm_schedule.FlightLegs` FL
    LEFT JOIN `afkl-dakota-p300.ds_kl_sm_schedule.LkpAirports` LKPA
        ON FL.ArrivalAirport = LKPA.IataAirportCode
       AND CURRENT_DATETIME() BETWEEN LKPA.DateFrom AND LKPA.DateUntil
    WHERE FL.HeadStationDepartureDateUtc BETWEEN '{start_date}' AND '{end_date}'
      AND FL.Airline IN ('KL', 'MP')
      AND NOT FL.IsDiverted
      AND FL.Suffix IS NULL
),

flight_plan_timestamped AS (
    SELECT
        CONCAT(
            CAST(headStationDepartureDateUtc AS STRING), '|',
            carrier, '|',
            LPAD(CAST(flightNumber AS STRING), 4, '0'), '|',
            '|',
            departureAirport
        )                                                               AS flight_key_id,
        (SELECT value FROM UNNEST(timestamps__scheduled_arrival_time)
            ORDER BY timestamp DESC LIMIT 1)                           AS fpts_scheduled_arrival_time,
        (SELECT value FROM UNNEST(timestamps__scheduled_departure_time)
            ORDER BY timestamp DESC LIMIT 1)                           AS fpts_scheduled_departure_time,
        DATETIME(
            TIMESTAMP(
                (SELECT value FROM UNNEST(timestamps__event_timestamp)
                    ORDER BY timestamp DESC LIMIT 1)
            )
        )                                                               AS fpts_event_timestamp
    FROM `afkl-dakota-p300.ds_kl_fo_atm_timestamped.ts_flight_plans`
    WHERE headStationDepartureDateUtc BETWEEN '{start_date}' AND '{end_date}'
      AND carrier IN ('KL', 'MP')
),

flight_details AS (
    SELECT
        FS.*,
        FPTS.fpts_event_timestamp,
        FPTS.fpts_scheduled_arrival_time,
        FPTS.fpts_scheduled_departure_time
    FROM flight_scope FS
    LEFT JOIN flight_plan_timestamped FPTS
        ON FS.flight_key_id = FPTS.flight_key_id
),

taf_raw AS (
    SELECT
        airport                                                         AS weather_airport,
        CAST(date AS DATE)                                              AS weather_date,
        CAST(hour AS INT64)                                             AS weather_hour,
        (SELECT value FROM UNNEST(forecast_taf__timestamp)
            ORDER BY timestamp DESC LIMIT 1)                           AS taf_timestamp,
        (SELECT value FROM UNNEST(forecast_taf__message)
            ORDER BY timestamp DESC LIMIT 1)                           AS taf_report
    FROM `afkl-dakota-p300.ds_kl_fo_atm_timestamped.ts_weather_by_station`
    WHERE CAST(date AS DATE) BETWEEN '{start_date}' AND '{end_date}'
),

joined AS (
    SELECT
        FD.*,
        TAF.taf_report,
        TAF.taf_timestamp
    FROM flight_details FD
    LEFT JOIN taf_raw TAF
        ON TAF.weather_airport = FD.fl_arrival_airport
       AND TAF.weather_date = DATE(FD.fpts_event_timestamp)
       AND TAF.weather_hour = EXTRACT(HOUR FROM FD.fpts_event_timestamp)
       AND TAF.taf_timestamp <= FD.fpts_event_timestamp
)

SELECT
    flight_key_id,
    fl_scheduled_arrival_time_utc,
    fl_actual_arrival_time_utc,
    fl_departure_airport,
    fl_arrival_airport,
    lkpa_arrival_airport_icao_code,
    fpts_event_timestamp,
    fpts_scheduled_arrival_time,
    fpts_scheduled_departure_time,
    taf_report,
    taf_timestamp,
    IF(taf_report IS NOT NULL, 1, 0)                                    AS has_taf_report
FROM joined
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY flight_key_id
    ORDER BY taf_timestamp DESC
) = 1