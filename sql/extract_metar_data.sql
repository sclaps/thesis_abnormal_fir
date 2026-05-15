WITH flight_details AS (
    SELECT
        FL.FlightLegUtcId                                               AS flight_key_id,
        FL.ScheduledArrivalTimeUtc                                      AS fl_scheduled_arrival_time_utc,
        FL.ActualArrivalTimeUtc                                         AS fl_actual_arrival_time_utc,
        FL.DepartureAirport                                             AS fl_departure_airport,
        FL.ArrivalAirport                                               AS fl_arrival_airport,
        LKPA.IcaoAirportCode                                            AS lkpa_arrival_airport_icao_code
    FROM `afkl-dakota-p300.ds_kl_sm_schedule.FlightLegs` FL
    LEFT JOIN `afkl-dakota-p300.ds_kl_sm_schedule.LkpAirports` LKPA
        ON  FL.ArrivalAirport = LKPA.IataAirportCode
        AND CURRENT_DATETIME() BETWEEN LKPA.DateFrom AND LKPA.DateUntil
    WHERE FL.HeadStationDepartureDateUtc BETWEEN '{start_date}' AND '{end_date}'
      AND FL.Airline IN ('KL', 'MP')
      AND NOT FL.IsDiverted
      AND FL.Suffix IS NULL
),

metar_raw AS (
    SELECT
        airport                                                         AS weather_airport,
        msg.timestamp                                                   AS metar_timestamp,
        msg.value                                                       AS metar_report,
        REGEXP_EXTRACT(msg.value, r'\w{4}\s+(\d{6})\s')                 AS metar_obs_token
    FROM `afkl-dakota-p300.ds_kl_fo_atm_timestamped.ts_weather_by_station`,
        UNNEST(actual_metar__message) AS msg
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
            AND REGEXP_EXTRACT(msg.value, r'\w{4}\s+(\d{6})\s') IS NOT NULL
),

metar AS (
    SELECT
        weather_airport,
        metar_timestamp,
        metar_report,
        TIMESTAMP(
            DATETIME_ADD(
                DATETIME_ADD(
                    CAST(
                        DATE_ADD(
                            CASE
                                WHEN CAST(SUBSTR(metar_obs_token, 1, 2) AS INT64)
                                    > EXTRACT(DAY FROM metar_timestamp) + 1
                                THEN DATE_TRUNC(DATE(metar_timestamp), MONTH) - INTERVAL 1 MONTH
                                ELSE DATE_TRUNC(DATE(metar_timestamp), MONTH)
                            END,
                            INTERVAL CAST(SUBSTR(metar_obs_token, 1, 2) AS INT64) - 1 DAY
                        )
                    AS DATETIME),
                    INTERVAL CAST(SUBSTR(metar_obs_token, 3, 2) AS INT64) HOUR
                ),
                INTERVAL CAST(SUBSTR(metar_obs_token, 5, 2) AS INT64) MINUTE
            )
        )                                                               AS metar_obs_timestamp
    FROM metar_raw
),

joined AS (
    SELECT
        FD.*,
        M.metar_report,
        M.metar_timestamp,
        M.metar_obs_timestamp,
        TIMESTAMP_DIFF(FD.fl_actual_arrival_time_utc, M.metar_obs_timestamp, MINUTE) AS metar_ata_diff_minutes,
        ABS(TIMESTAMP_DIFF(M.metar_obs_timestamp, FD.fl_actual_arrival_time_utc, MINUTE))
                                                                        AS metar_t0_target_diff_minutes,
        ABS(TIMESTAMP_DIFF(M.metar_obs_timestamp, TIMESTAMP_SUB(FD.fl_actual_arrival_time_utc, INTERVAL 30 MINUTE), MINUTE))
                                                                        AS metar_t30_target_diff_minutes

    FROM flight_details FD
    LEFT JOIN metar M
        ON  M.weather_airport       = FD.fl_arrival_airport
        AND M.metar_obs_timestamp BETWEEN
                TIMESTAMP_SUB(FD.fl_actual_arrival_time_utc, INTERVAL 60 MINUTE)
            AND FD.fl_actual_arrival_time_utc
)

SELECT
    flight_key_id,
    fl_scheduled_arrival_time_utc,
    fl_actual_arrival_time_utc,
    fl_departure_airport,
    fl_arrival_airport,
    lkpa_arrival_airport_icao_code,

    -- t0: closest METAR to actual arrival
    FIRST_VALUE(metar_report)       OVER w_t0                          AS klm_t0_metar_report,
    FIRST_VALUE(metar_timestamp)    OVER w_t0                          AS klm_t0_metar_timestamp,
    FIRST_VALUE(metar_obs_timestamp) OVER w_t0                         AS klm_t0_metar_obs_timestamp,
    FIRST_VALUE(metar_t0_target_diff_minutes) OVER w_t0                AS klm_t0_metar_target_diff_minutes,
    FIRST_VALUE(metar_ata_diff_minutes) OVER w_t0                      AS klm_t0_metar_ata_diff_minutes,
    IF(FIRST_VALUE(metar_report) OVER w_t0 IS NOT NULL, 1, 0)          AS has_klm_t0_metar_report,

    -- t30: closest METAR to 30 min before actual arrival
    FIRST_VALUE(metar_report)       OVER w_t30                         AS klm_t30_metar_report,
    FIRST_VALUE(metar_timestamp)    OVER w_t30                         AS klm_t30_metar_timestamp,
    FIRST_VALUE(metar_obs_timestamp) OVER w_t30                        AS klm_t30_metar_obs_timestamp,
    FIRST_VALUE(metar_t30_target_diff_minutes) OVER w_t30              AS klm_t30_metar_target_diff_minutes,
    FIRST_VALUE(metar_ata_diff_minutes) OVER w_t30                     AS klm_t30_metar_ata_diff_minutes,
    IF(FIRST_VALUE(metar_report) OVER w_t30 IS NOT NULL, 1, 0)         AS has_klm_t30_metar_report

FROM joined
QUALIFY ROW_NUMBER() OVER (PARTITION BY flight_key_id ORDER BY metar_t0_target_diff_minutes ASC) = 1

WINDOW
    w_t0  AS (PARTITION BY flight_key_id ORDER BY metar_t0_target_diff_minutes  ASC),
    w_t30 AS (PARTITION BY flight_key_id ORDER BY metar_t30_target_diff_minutes ASC)