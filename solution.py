#Number of flights departed from each of the state.

flight_count_per_state = joined_flights.groupBy("State").agg(count(lit(1)).alias("FlightCount")).orderBy(col("FlightCount").desc())
flight_count_per_state.show()

#Geting the list of airports in the US from which flights are not departed.
airports_not_used = joined_flights.filter(airlines.Origin.isNull()).select('City', 'State', 'Country', 'IATA')
airports_not_used.count()
