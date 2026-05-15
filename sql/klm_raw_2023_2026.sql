SELECT
    FL.FlightLegUtcId AS flight_leg_key, 
    FL.FlightPlanVersion AS fl_flight_plan_version,
    FL.HeadStationDepartureDateUtc AS fl_scheduled_departure_date_utc, 
    FL.ScheduledArrivalDateUtc AS fl_scheduled_arrival_date_utc, 
    FL.Season AS fl_season, 
    FL.Airline AS fl_airline, 
    FL.FlightNumber AS fl_flight_number, 
    FL.FlightCode AS fl_flight_code, 
    FL.ServiceType AS fl_service_type, 
    FL.FlightProduct AS fl_flight_product, 
    FL.CityPair AS fl_city_pair, 
    FL.Direction AS fl_direction, 
    FL.DepartureAirport AS fl_departure_airport, 
    LKPA_Dep.IcaoAirportCode AS fl_departure_airport_icao_code, 
    LKPA_Dep.Latitude AS fl_departure_airport_latitude, 
    LKPA_Dep.Longitude AS fl_departure_airport_longitude, 
    FL.ActualArrivalAirport AS fl_actual_arrival_airport, 
    FL.PlannedArrivalAirport AS fl_planned_arrival_airport, 
    LKPA_Arr.IcaoAirportCode AS fl_arrival_airport_icao_code, 
    LKPA_Arr.Latitude AS fl_arrival_airport_latitude, 
    LKPA_Arr.Longitude AS fl_arrival_airport_longitude,  
    FL.AircraftType AS fl_aircraft_type, 
    FL.AircraftSubType AS fl_aircraft_subtype, 
    FL.AircraftRegistration AS fl_aircraft_registration, 
    FL.AircraftOwner AS fl_aircraft_owner, 
    FL.SOBT AS fl_scheduled_off_block_time_utc, 
    FL.AOBT AS fl_actual_off_block_time_utc, 
    FL.SIBT AS fl_scheduled_in_block_time_utc, 
    FL.AIBT AS fl_actual_in_block_time_utc, 
    FL.ScheduledBlockTimeUtc AS fl_scheduled_block_time_min, 
    FL.ActualBlockTimeUtc AS fl_actual_block_time_min, 
    FL.ActualTaxiOutDuration AS fl_actual_taxi_out_duration_min, 
    FL.FlightPlanTripDuration AS fl_flight_plan_trip_duration_min, 
    FL.ActualTripDuration AS fl_actual_trip_duration_min, 
    FL.ActualTaxiInDuration AS fl_actual_taxi_in_duration_min, 
    FL.DepartureDelayDifference AS fl_departure_delay_difference_min, 
    FL.BlockDelayDifference AS fl_block_delay_difference_min, 
    FL.ArrivalDelayDifference AS fl_arrival_delay_difference_min, 
    FL.FlightPlanSTAR AS fl_star,
    FL.FlightPlanDepartureRunway AS fl_departure_runway,
    FL.FlightPlanArrivalRunway AS fl_arrival_runway,
    FL.ATDT AS fl_actual_touchdown_time_utc,
    FLS.PreviousLegForAircraft AS fl_prev_leg,
    FLS.PreviousLegScheduledArrivalTimeAtHovUtc AS fl_prev_leg_sibt_utc,
    FLS.PreviousLegActualArrivalTimeUtc AS fl_prev_leg_aibt_utc,
    CASE 
        WHEN FLS.PreviousLegActualArrivalTimeUtc IS NULL OR FLS.PreviousLegScheduledArrivalTimeAtHovUtc IS NULL THEN NULL
        ELSE TIMESTAMP_DIFF(FLS.PreviousLegActualArrivalTimeUtc,FLS.PreviousLegScheduledArrivalTimeAtHovUtc,MINUTE)
    END AS fl_prev_leg_arrival_delay_min,
    CASE 
        WHEN FL.ActualTripDuration IS NULL OR FL.FlightPlanTripDuration IS NULL THEN NULL 
        WHEN FL.ActualTripDuration - FL.FlightPlanTripDuration = 0 THEN 0 
        ELSE FL.ActualTripDuration - FL.FlightPlanTripDuration 
    END AS fl_trip_duration_deviation_min,  
    FP.FlightPlanVersion AS fp_flight_plan_version, 
    FP.FlightPlanTimestamp AS fp_flight_plan_timestamp_utc, 
    FP.PlannedArrivalRunway AS fp_planned_arrival_runway,  
    FL.GreatCircleDistance AS fl_great_circle_distance_nm,   
    FP.CostIndex AS fp_cost_index,    
    FP.RouteWaypoints AS fp_route_waypoints, 
    FLS.EstimatedTouchDownTimeUtc AS fl_estimated_touchdown_time_utc,
    FLS.ActualTouchDownTimeUtc AS fl_actual_touchdown_time_utc,
    CASE 
        WHEN FLS.ActualTouchDownTimeUtc IS NULL OR FLS.EstimatedTouchDownTimeUtc IS NULL THEN NULL
        ELSE TIMESTAMP_DIFF(FLS.ActualTouchDownTimeUtc,FLS.EstimatedTouchDownTimeUtc,MINUTE)
    END AS fl_touchdown_delay_min,
    CASE WHEN FP.FlightLegUtcLegId IS NULL THEN 0 ELSE 1 END AS has_flight_plan_data,

FROM `afkl-skyarch-p200.ds_refined_mart_atm.KlmFlightLegs` FL

LEFT JOIN `afkl-dakota-p300.ds_kl_fo_atm.FlightPlan` FP
    ON FL.FlightLegUtcLegId = FP.FlightLegUtcLegId
    -- AND FlightPlanVersion !! 

LEFT JOIN `afkl-dakota-p300.ds_kl_sm_schedule.LkpAirports` LKPA_Dep
    ON FL.DepartureAirport = LKPA_Dep.IataAirportCode

LEFT JOIN `afkl-dakota-p300.ds_kl_sm_schedule.LkpAirports` LKPA_Arr
    ON FL.PlannedArrivalAirport = LKPA_Arr.IataAirportCode

LEFT JOIN `afkl-dakota-p300.ds_kl_sm_schedule.LkpRegionNames` LKPAR
    ON FL.DepartureAirport = LKPAR.DepartureStationCodeIATA
   AND FL.PlannedArrivalAirport = LKPAR.ArrivalStationCodeIATA

LEFT JOIN `afkl-dakota-p300.ds_kl_sm_schedule.FlightLegs` FLS
    ON FL.FlightLegUtcId = FLS.FlightLegUtcId
    AND FLS.HeadStationDepartureDateUtc BETWEEN DATE('{start_date}') AND DATE('{end_date}')

WHERE 1=1
    AND FL.HeadStationDepartureDateUtc BETWEEN DATE('{start_date}') AND DATE('{end_date}')
    AND FL.Airline IN ('KL', 'MP')
    AND (FL.ActualArrivalAirport = 'AMS' OR FL.DepartureAirport = 'AMS')
    AND NOT FL.IsDiverted
    AND FL.PlannedArrivalAirport = FL.ActualArrivalAirport
    AND (FL.Suffix IS NULL OR FL.Suffix IN ('D', 'A'))

    -- AND FL.ServiceType = 'J'

    -- AND FL.FlightLegUtcId IS NOT NULL
    -- AND FL.FlightNumber IS NOT NULL
    -- AND FL.FlightCode IS NOT NULL
    -- AND FL.SIBT IS NOT NULL
    -- AND FL.AIBT IS NOT NULL

    -- AND FL.BlockDelayDifference < 60

    AND CURRENT_DATETIME() BETWEEN LKPA_Dep.DateFrom AND LKPA_Dep.DateUntil
    AND CURRENT_DATETIME() BETWEEN LKPA_Arr.DateFrom AND LKPA_Arr.DateUntil