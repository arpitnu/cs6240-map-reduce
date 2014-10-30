REGISTER file:/home/hadoop/lib/pig/piggybank.jar;

-- CSVLoader for reading data from input file
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

-- Set default parallel as 20
SET default_parallel 20;

-- Load	the	flights	file as	Flights1. 
Flights1 = LOAD '$INPUT' USING CSVLoader() AS (Year:int,Quarter,Month:int,DayofMonth,DayOfWeek,FlightDate,UniqueCarrier,
AirlineID,Carrier,TailNum,FlightNum,Origin,OriginCityName,OriginState,OriginStateFips,
OriginStateName,OriginWac,Dest,DestCityName,DestState,DestStateFips,DestStateName,DestWac,
CRSDepTime,DepTime:int,DepDelay,DepDelayMinutes,DepDel15,DepartureDelayGroups,DepTimeBlk,
TaxiOut,WheelsOff,WheelsOn,TaxiIn,CRSArrTime,ArrTime:int,ArrDelay,ArrDelayMinutes:int,
ArrDel15,ArrivalDelayGroups,ArrTimeBlk,Cancelled,CancellationCode,Diverted,CRSElapsedTime,
ActualElapsedTime,AirTime,Flights,Distance,DistanceGroup,CarrierDelay,WeatherDelay,NASDelay,
SecurityDelay,LateAircraftDelay);

-- Load	the	flights	file as	Flights2.  
Flights2 = LOAD '$INPUT' USING CSVLoader() AS (Year:int,Quarter,Month:int,DayofMonth,DayOfWeek,FlightDate,UniqueCarrier,
AirlineID,Carrier,TailNum,FlightNum,Origin,OriginCityName,OriginState,OriginStateFips,
OriginStateName,OriginWac,Dest,DestCityName,DestState,DestStateFips,DestStateName,DestWac,
CRSDepTime,DepTime:int,DepDelay,DepDelayMinutes,DepDel15,DepartureDelayGroups,DepTimeBlk,
TaxiOut,WheelsOff,WheelsOn,TaxiIn,CRSArrTime,ArrTime:int,ArrDelay,ArrDelayMinutes:int,
ArrDel15,ArrivalDelayGroups,ArrTimeBlk,Cancelled,CancellationCode,Diverted,CRSElapsedTime,
ActualElapsedTime,AirTime,Flights,Distance,DistanceGroup,CarrierDelay,WeatherDelay,NASDelay,
SecurityDelay,LateAircraftDelay);

-- Remove unwanted records from Flights1
Flight1Data = FOREACH Flights1 GENERATE Year, Month, FlightDate, Origin, Dest, DepTime, ArrTime, ArrDelayMinutes, Cancelled, Diverted;

-- Remove unwanted records from Flights2
Flight2Data = FOREACH Flights2 GENERATE Year, Month, FlightDate, Origin, Dest, DepTime, ArrTime, ArrDelayMinutes, Cancelled, Diverted;

-- Remove as many records and attributes from Flights1 & Flights2 as possible, with the condition flight date is valid
firstFlights = FILTER Flight1Data BY ((Origin == 'ORD' AND Dest != 'JFK') AND 
                                        Cancelled == '0.00' AND Diverted == '0.00' AND 
                                        ((Year == 2007 and Month >= 6) OR
									     (Year == 2008 and Month <= 5)));
secondFlights = FILTER Flight2Data BY ((Origin != 'ORD' AND Dest == 'JFK') AND 
                                        Cancelled == '0.00' AND Diverted == '0.00' AND 
                                         ((Year == 2007 and Month >= 6) OR
									     (Year == 2008 and Month <= 5)));  
jointFlights = JOIN firstFlights BY (Dest, FlightDate), secondFlights BY (Origin, FlightDate);

-- Filter out those	join tuples	where the departure	time in	Flights2 is	not	after the arrival time in Flights1.
filteredJointFlights = FILTER jointFlights BY (firstFlights::ArrTime < secondFlights::DepTime);

-- Average calculation
arrDelays = FOREACH filteredJointFlights GENERATE (firstFlights::ArrDelayMinutes + secondFlights::ArrDelayMinutes) AS delay;
groupedArrDelays = GROUP arrDelays ALL;
avgDelay = FOREACH groupedArrDelays GENERATE AVG(arrDelays);

-- Store Delay
STORE avgDelay INTO '$OUTPUT';

--DUMP avgDelay;
