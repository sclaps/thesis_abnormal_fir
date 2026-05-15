SELECT 
    FlightLegUtcId,
    AircraftType,
    AircraftSubType,
    AircraftRegistration,
    DepartureAirport,
    ActualArrivalAirport,
    SOBT,
    'local is lekker' AS Phillipe
FROM `afkl-skyarch-p200.ds_refined_mart_atm.KlmFlightLegs`
WHERE 1=1
    AND FlightLegUtcId >= '2026-03-01'
ORDER BY SDD DESC, FlightLegUtcId ASC