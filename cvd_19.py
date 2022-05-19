import pandas as pd
import psycopg2 as ps


#connect to database
con = ps.connect(host = "localhost",
                 database = "covid_19",
                 user = "postgres",
                 password = "your_password")

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
data['newcases']=data['newcases'].astype("int")
data['totaldeaths']=data['totaldeaths'].astype("int")
data['newdeaths']=data['newdeaths'].astype("int")
data['totalrecovered']=data['totalrecovered'].astype("int")
data['newrecovered']=data['newrecovered'].astype("int")
data['activecases']=data['activecases'].astype("int")
data['serious_critical']=data['serious_critical'].astype("int")
data['tot_cases_1m_pop']=data['tot_cases_1m_pop'].astype("int")
data['deaths_1m_pop']=data['deaths_1m_pop'].astype("int")
data['totaltests']=data['totaltests'].astype("int")
data['tests_1m_pop']=data['tests_1m_pop'].astype("int")

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
   country_region CHAR(20),
   continent CHAR(20) NOT NULL,
   population BIGINT,
   totalcases BIGINT,  
   newcases BIGINT,
   totaldeaths BIGINT,
   newdeaths BIGINT,
   totalrecovered BIGINT,
   newrecovered BIGINT,
   activecases BIGINT,
   serious_critical BIGINT,
   tot_cases_1m_pop BIGINT,
   deaths_1m_pop BIGINT,   
   totaltests BIGINT,
   tests_1m_pop BIGINT,
   who_region CHAR(20)
)'''

cases_table = ''' CREATE TABLE cases_table(
   country_region CHAR(20),
   totalcases BIGINT,  
   newcases BIGINT,
   activecases BIGINT,
   tot_cases_1m_pop BIGINT
)'''

deaths_and_recovered_table = ''' CREATE TABLE deaths_and_recovered_table(
   country_region CHAR(20),
   totaldeaths BIGINT,
   newdeaths BIGINT,
   totalrecovered BIGINT,
   newrecovered BIGINT,
   serious_critical BIGINT,
   deaths_1m_pop BIGINT
)'''

location_table = ''' CREATE TABLE location_table(
   country_region TEXT,
   continent TEXT NOT NULL,
   population TEXT
)'''

#cur.execute(data_table)
#cur.execute(cases_table)
#cur.execute(deaths_and_recovered_table)
#cur.execute(location_table)

#transfer the data
for i in range(0,row_num):
    cur.execute("INSERT INTO location_table(country_region,continent,population) VALUES(%s,%s,%s)",
                (data.iloc[i,0],data.iloc[i,1],data.iloc[i,2]))


#sorguyu ve kursorü kapat ardından bağlantıyı sonlandır.
con.commit()
#cur.close()
#con.close()