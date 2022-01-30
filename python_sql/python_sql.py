#Creating database
import sqlite3
conn = sqlite3.connect('airline2.db')

#Creating ontime table
import pandas as pd
flight_2000 = pd.read_csv('2000.csv')
flight_2001 = pd.read_csv('2001.csv')
flight_2002 = pd.read_csv('2002.csv')
flight_2003 = pd.read_csv('2003.csv')
flight_2004 = pd.read_csv('2004.csv')
flight_2005 = pd.read_csv('2005.csv')
print(flight_2000.index)
print(flight_2000.columns)

flight_2000.to_sql('ontime', con = conn, index = False)
flight_2001.to_sql('ontime', con = conn, index = False, if_exists = 'append')
flight_2002.to_sql('ontime', con = conn, index = False, if_exists = 'append')
flight_2003.to_sql('ontime', con = conn, index = False, if_exists = 'append')
flight_2004.to_sql('ontime', con = conn, index = False, if_exists = 'append')
flight_2005.to_sql('ontime', con = conn, index = False, if_exists = 'append')

#Creating airports, carriers, and planes table
airports = pd.read_csv('airports.csv')
print(airports.index)
print(airports.columns)

carriers = pd.read_csv('carriers.csv')
print(carriers.index)
print(carriers.columns)

planes = pd.read_csv('plane-data.csv')
print(planes.index)
print(planes.columns)

airports.to_sql('airports', con = conn, index = False)
carriers.to_sql('carriers', con = conn, index = False)
planes.to_sql('planes', con = conn, index = False)

c = conn.cursor()
c.execute('SELECT name FROM sqlite_master WHERE type="table"')
c.fetchall()

#Querying database
#Which of the following companies has the highest number of cancelled flights, relative to their number of total flights?
q1 = c.execute('SELECT carriers.Description, COUNT(*) AS "Total_Flights", SUM(Cancelled) AS "Cancelled_Flights" FROM ontime JOIN carriers ON ontime.UniqueCarrier = carriers.Code WHERE carriers.Description = "United Air Lines Inc." OR carriers.Description = "American Airlines Inc." OR carriers.Description = "Pinnacle Airlines Inc." OR carriers.Description = "Delta Air Lines Inc." GROUP BY carriers.Description').fetchall()
pd.DataFrame(q1)
q1 = pd.DataFrame(q1)
q1 = q1.rename(columns={0: 'Description', 1: 'Total_Flights', 2: 'Cancelled_Flights'})
q1

#Which of the following airplanes has the lowest associated average departure delay (excluding cancelled and diverted flights)?
q2 = c.execute('SELECT planes.model, AVG(ontime.DepDelay) AS "Avg_Dep_Delay" FROM ontime JOIN planes ON ontime.TailNum = planes.tailnum WHERE (planes.model = "737-230" OR planes.model = "ERJ 190-100 IGW" OR planes.model = "A330-223" OR planes.model = "737-282") AND (ontime.Cancelled != 1 AND ontime.Diverted != 1) GROUP BY planes.model ORDER BY AVG(ontime.DepDelay)').fetchall()
pd.DataFrame(q2)
q2 = pd.DataFrame(q2)
q2 = q2.rename(columns={0: 'Model', 1: 'Avg_Dep_Delay'})
q2

#Which of the following companies has the highest number of cancelled flights?
q3 = c.execute('SELECT carriers.Description, SUM(ontime.Cancelled) AS "Cancelled_Flights" FROM ontime JOIN carriers ON ontime.UniqueCarrier = carriers.Code WHERE carriers.Description = "United Air Lines Inc." OR carriers.Description = "American Airlines Inc." OR carriers.Description = "Pinnacle Airlines Inc." OR carriers.Description = "Delta Air Lines Inc." GROUP BY carriers.Description ORDER BY SUM(ontime.Cancelled) DESC').fetchall()
pd.DataFrame(q3)
q3 = pd.DataFrame(q3)
q3 = q3.rename(columns={0: 'Description', 1: 'Cancelled_Flights'})
q3

#Which of the following cities has the highest number of inbound flights (excluding cancelled flights)?
q4 = c.execute('SELECT airports.city, COUNT(*) AS "Inbound_Flights" FROM ontime JOIN airports ON ontime.dest = airports.iata WHERE (airports.city = "Chicago" OR airports.city = "Atlanta" OR airports.city = "New York" OR airports.city = "Houston") AND ontime.Cancelled = 0 GROUP BY airports.city ORDER BY COUNT(*) DESC').fetchall()
pd.DataFrame(q4)
q4 = pd.DataFrame(q4)
q4 = q4.rename(columns={0: 'City', 1: 'Inbound_Flights'})
q4

q1.to_csv('q1.csv', index = False)
q2.to_csv('q2.csv', index = False)
q3.to_csv('q3.csv', index = False)
q4.to_csv('q4.csv', index = False)

conn.close()