import pandas as pd
import psycopg2 as ps


#connect to database
con = ps.connect(host = "localhost",
                 database = "covid_19",
                 user = "postgres",
                 password = "yourpassword")

#create the cursor
cur = con.cursor()

#importing the data
data = pd.read_csv("data_covid.csv", sep=";")
#rename the data
data.rename(columns = {'Country/Region':'country_region','Continent':'continent','Population':'population',
                       'TotalCases':'totalcases','NewCases':'newcases','TotalDeaths':'totaldeaths',
                       'NewDeaths':'newdeaths','TotalRecovered':'totalrecovered','NewRecovered':'newrecovered',
                       'ActiveCases':'activecases','Serious,Critical':'serious_critical',
                       'Tot Cases/1M pop':'tot_cases_1m_pop', 'Deaths/1M pop':'deaths_1m_pop','TotalTests':'totaltests',
                       'Tests/1M pop':'tests_1m_pop','WHO Region':'who_region'}, inplace = True)
data = data.fillna(0)


data['population']=data['population'].astype("str")
data['totalcases']=data['totalcases'].astype("str")
data['newcases']=data['newcases'].astype("str")
data['totaldeaths']=data['totaldeaths'].astype("str")
data['newdeaths']=data['newdeaths'].astype("str")
data['totalrecovered']=data['totalrecovered'].astype("str")
data['newrecovered']=data['newrecovered'].astype("str")
data['activecases']=data['activecases'].astype("str")
data['serious_critical']=data['serious_critical'].astype("str")
data['tot_cases_1m_pop']=data['tot_cases_1m_pop'].astype("str")
data['deaths_1m_pop']=data['deaths_1m_pop'].astype("str")
data['totaltests']=data['totaltests'].astype("str")
data['tests_1m_pop']=data['tests_1m_pop'].astype("str")

#seperate the data
cases_data = data[['country_region','totalcases','newcases','activecases','tot_cases_1m_pop']]

deaths_and_recovered_data = data[['country_region','totaldeaths', 'newdeaths', 'totalrecovered',
                                 'newrecovered','serious_critical','deaths_1m_pop']]

location_data = data[['country_region','continent','population']]

row_num, col_num = data.shape

row_num_cases, col_num_cases = cases_data.shape

row_num_deaths, col_num_deaths = deaths_and_recovered_data.shape

row_num_location, col_num_location = location_data.shape


#create a table

data_table = ''' CREATE TABLE data_table(
   country_region TEXT,
   continent CHAR(30) NOT NULL,
   population TEXT,
   totalcases TEXT,  
   newcases TEXT,
   totaldeaths TEXT,
   newdeaths TEXT,
   totalrecovered TEXT,
   newrecovered TEXT,
   activecases TEXT,
   serious_critical TEXT,
   tot_cases_1m_pop TEXT,
   deaths_1m_pop TEXT,   
   totaltests TEXT,
   tests_1m_pop TEXT,
   who_region TEXT
)'''

cases_table = ''' CREATE TABLE cases_table(
   country_region CHAR(40),
   totalcases TEXT,  
   newcases TEXT,
   activecases TEXT,
   tot_cases_1m_pop TEXT
)'''

deaths_and_recovered_table = ''' CREATE TABLE deaths_and_recovered_table(
   country_region CHAR(40),
   totaldeaths TEXT,
   newdeaths TEXT,
   totalrecovered TEXT,
   newrecovered TEXT,
   serious_critical TEXT,
   deaths_1m_pop TEXT
)'''

location_table = ''' CREATE TABLE location_table(
   country_region TEXT,
   continent TEXT NOT NULL,
   population TEXT
)'''

cur.execute(data_table)
cur.execute(cases_table)
cur.execute(deaths_and_recovered_table)
cur.execute(location_table)

#transfer the data from data_table
for i in range(0,row_num):
    cur.execute("INSERT INTO data_table(country_region,continent,population,totalcases,newcases,totaldeaths,newdeaths,totalrecovered,newrecovered,activecases,serious_critical,tot_cases_1m_pop,deaths_1m_pop,totaltests,tests_1m_pop,who_region) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
               (data.iloc[i,0],data.iloc[i,1],data.iloc[i,2],data.iloc[i,3],data.iloc[i,4],data.iloc[i,5],data.iloc[i,6],data.iloc[i,7],data.iloc[i,8],data.iloc[i,9],data.iloc[i,10],data.iloc[i,11],data.iloc[i,12],data.iloc[i,13],data.iloc[i,14],data.iloc[i,15]))

#transfer the data from cases_table
for i in range(0,row_num_cases):
    cur.execute("INSERT INTO cases_table(country_region,totalcases,newcases,activecases,tot_cases_1m_pop) VALUES(%s,%s,%s,%s,%s)",
               (data.iloc[i,0],data.iloc[i,3],data.iloc[i,4],data.iloc[i,9],data.iloc[i,11]))

#transfer the data from deaths_and_recovered_table
for i in range(0,row_num_deaths):
    cur.execute("INSERT INTO deaths_and_recovered_table(country_region,totaldeaths,newdeaths,totalrecovered,newrecovered,serious_critical,deaths_1m_pop) VALUES(%s,%s,%s,%s,%s,%s,%s)",
               (data.iloc[i,0],data.iloc[i,1],data.iloc[i,2],data.iloc[i,3],data.iloc[i,4],data.iloc[i,5],data.iloc[i,6]))

#transfer the data from location_table
for i in range(0,row_num_location):
    cur.execute("INSERT INTO location_table(country_region,continent,population) VALUES(%s,%s,%s)",
                (data.iloc[i,0],data.iloc[i,1],data.iloc[i,2]))


#sorguyu ve kursorü kapat ardından bağlantıyı sonlandır.
con.commit()
cur.close()
con.close()