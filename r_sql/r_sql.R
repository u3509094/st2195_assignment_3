#Creating database
library(DBI)
conn <- dbConnect(RSQLite::SQLite(), 'airline2.db')

flight_2000 <- read.csv('2000.csv')
flight_2001 <- read.csv('2001.csv')
flight_2002 <- read.csv('2002.csv')
flight_2003 <- read.csv('2003.csv')
flight_2004 <- read.csv('2004.csv')
flight_2005 <- read.csv('2005.csv')

airports <- read.csv('airports.csv')
carriers <- read.csv('carriers.csv')
planes <- read.csv('plane-data.csv')

#Creating ontime table
dbWriteTable(conn, 'ontime', flight_2000)
dbAppendTable(conn, 'ontime', flight_2001)
dbAppendTable(conn, 'ontime', flight_2002)
dbAppendTable(conn, 'ontime', flight_2003)
dbAppendTable(conn, 'ontime', flight_2004)
dbAppendTable(conn, 'ontime', flight_2005)

dbListTables(conn)
dbListFields(conn, 'ontime')
dbGetQuery(conn, 'SELECT * FROM ontime LIMIT 5')

#Creating airports, carriers, and planes table
dbWriteTable(conn, 'airports', airports)
dbWriteTable(conn, 'carriers', carriers)
dbWriteTable(conn, 'planes', planes)

dbListTables(conn)
dbListFields(conn, 'airports')
dbGetQuery(conn, 'SELECT * FROM airports LIMIT 5')
dbListFields(conn, 'carriers')
dbGetQuery(conn, 'SELECT * FROM carriers LIMIT 5')
dbListFields(conn, 'planes')
dbGetQuery(conn, 'SELECT * FROM planes LIMIT 5')

#Querying database
library(dplyr)
ontime_db <- tbl(conn, 'ontime')
airports_db <- tbl(conn, 'airports')
carriers_db <- tbl(conn, 'carriers')
planes_db <- tbl(conn, 'planes')

#Which of the following companies has the highest number of cancelled flights, relative to their number of total flights?
#DBI
q1_DBI <- dbGetQuery(conn,'
                 SELECT carriers.Description, COUNT(*) AS "Total_Flights", SUM(Cancelled) AS "Cancelled_Flights"
                 FROM ontime JOIN carriers ON ontime.UniqueCarrier = carriers.Code
                 WHERE carriers.Description = "United Air Lines Inc." OR carriers.Description = "American Airlines Inc." OR carriers.Description = "Pinnacle Airlines Inc." OR carriers.Description = "Delta Air Lines Inc."
                 GROUP BY carriers.Description
                 ')
q1_DBI

q1_DBI['Cancellation_Percertage'] = q1_DBI['Cancelled_Flights']/q1_DBI['Total_Flights']
q1_DBI <- arrange(q1_DBI, desc(Cancellation_Percertage))
q1_DBI

#dplyr
q1_dplyr <- ontime_db %>% inner_join(carriers_db, by = c("UniqueCarrier" = "Code")) %>%
  filter(Description %in% c("United Air Lines Inc.", "American Airlines Inc.", "Pinnacle Airlines Inc.", "Delta Air Lines Inc.")) %>%
  group_by(Description) %>%
  summarise(Total_Flights = n(), Cancelled_Flights = sum(Cancelled == 1, na.rm = TRUE), Cancellation_Percertage = mean(Cancelled == 1, na.rm = TRUE)) %>%
  arrange(desc(Cancellation_Percertage))
q1_dplyr

#Which of the following airplanes has the lowest associated average departure delay (excluding cancelled and diverted flights)?
#DBI
q2_DBI <- dbGetQuery(conn,'
                 SELECT planes.model, AVG(ontime.DepDelay) AS "Avg_Dep_Delay"
                 FROM ontime JOIN planes ON ontime.TailNum = planes.tailnum
                 WHERE (planes.model = "737-230" OR planes.model = "ERJ 190-100 IGW" OR planes.model = "A330-223" OR planes.model = "737-282") AND (ontime.Cancelled != 1 AND ontime.Diverted != 1)
                 GROUP BY planes.model
                 ORDER BY AVG(ontime.DepDelay)
                 ')
q2_DBI

#dplyr
q2_dplyr <- ontime_db %>% rename(year = Year, tailnum = TailNum) %>%
  inner_join(planes_db, by = "tailnum") %>%
  filter(model %in% c("737-230", "ERJ 190-100 IGW", "A330-223", "737-282") & (Cancelled != 1 & Diverted != 1)) %>%
  group_by(model) %>%
  summarise(Avg_Dep_Delay = mean(DepDelay, na.rm = TRUE)) %>%
  arrange(Avg_Dep_Delay)
q2_dplyr

#Which of the following companies has the highest number of cancelled flights?
#DBI
q3_DBI <- dbGetQuery(conn,'
                 SELECT carriers.Description, SUM(ontime.Cancelled) AS "Cancelled_Flights"
                 FROM ontime JOIN carriers ON ontime.UniqueCarrier = carriers.Code
                 WHERE carriers.Description = "United Air Lines Inc." OR carriers.Description = "American Airlines Inc." OR carriers.Description = "Pinnacle Airlines Inc." OR carriers.Description = "Delta Air Lines Inc."
                 GROUP BY carriers.Description
                 ORDER BY SUM(ontime.Cancelled) DESC
                 ')
q3_DBI

#dplyr
q3_dplyr <- ontime_db %>% inner_join(carriers_db, by = c("UniqueCarrier" = "Code")) %>%
  filter(Description %in% c("United Air Lines Inc.", "American Airlines Inc.", "Pinnacle Airlines Inc.", "Delta Air Lines Inc.")) %>%
  group_by(Description) %>%
  summarise(Cancelled_Flights = sum(Cancelled, na.rm = TRUE)) %>%
  arrange(desc(Cancelled_Flights))
q3_dplyr

#Which of the following cities has the highest number of inbound flights (excluding cancelled flights)?
#DBI
q4_DBI <- dbGetQuery(conn,'
                 SELECT airports.city, COUNT(*) AS "Inbound_Flights"
                 FROM ontime JOIN airports ON ontime.dest = airports.iata
                 WHERE (airports.city = "Chicago" OR airports.city = "Atlanta" OR airports.city = "New York" OR airports.city = "Houston") AND ontime.Cancelled = 0
                 GROUP BY airports.city
                 ORDER BY COUNT(*) DESC
                 ')
q4_DBI

#dplyr
q4_dplyr <- ontime_db %>% inner_join(airports_db, by = c("Dest" = "iata")) %>%
  filter(city %in% c("Chicago", "Atlanta", "New York", "Houston") & Cancelled == 0) %>%
  group_by(city) %>%
  summarise(Inbound_Flights = n()) %>%
  arrange(desc(Inbound_Flights))
q4_dplyr

#Writing queries into csv
write.csv(q1_DBI, 'q1.csv', row.names = FALSE)
write.csv(q2_DBI, 'q2.csv', row.names = FALSE)
write.csv(q3_DBI, 'q3.csv', row.names = FALSE)
write.csv(q4_DBI, 'q4.csv', row.names = FALSE)

#Disconnecting database
dbDisconnect(conn)