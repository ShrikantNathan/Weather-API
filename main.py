import os
import psycopg2
import numpy as np
from typing import List, Union, AnyStr

class WeatherDataAPI:
    def __init__(self):
        self.connector = psycopg2.connect(database="postgres", user="postgres", password="Lotus@123", host="localhost",
                                     port=5432)
        self.cursor = self.connector.cursor()
        print('Connected to Server ->', not self.cursor.closed)

    def connect_server_and_ingest_data(self):
        try:
            for wx_file in os.listdir(os.path.join(os.getcwd(), 'code-challenge-template', 'wx_data')):
                with open(os.path.join(os.getcwd(), "code-challenge-template", "wx_data", wx_file), 'r') as file:
                    # cursor.copy_from(file, 'tbl_weather', sep=',')
                    for line in file.readlines():
                        filtered = line.strip().split()
                        self.cursor.execute("INSERT INTO tbl_weather VALUES (TO_DATE(%s, 'YYYYMMDD'), %s, %s, %s)", (filtered[0], filtered[1], filtered[2], filtered[3]))
                        # print(filtered[0])
                self.connector.commit()
            print('Data inserted successfully')

        except (Exception, psycopg2.Error) as e:
            print('Error while inserting data into the server', e)

        finally:
            if self.connector:
                self.cursor.close()
                self.connector.close()
                print('Disconnected', self.connector.closed)

    def fetch_all_weather_records(self):
        self.cursor.execute("SELECT * FROM tbl_weather")
        record = self.cursor.fetchall()
        for rec in record:
            print(rec[0], rec[1], rec[2], rec[3])

    def fetch_distinct_weather_records_and_calculate(self):
        """ This action will loop over the table, fetch distinct rows, and perform every calculations on each
        rows and insert it to the database"""
        import operator
        self.cursor.execute("SELECT DISTINCT w_date, max_temp, min_temp, amt_precipitat FROM tbl_weather;")
        record = self.cursor.fetchall()
        for rec in record:
            w_date, max_temp, min_temp, amt_precip = rec[0], rec[1], rec[2], rec[3]
            temp_fahrenheit = operator.add(operator.mul(1.8, max_temp), 32)
            temp_celsius = operator.truediv(operator.sub(temp_fahrenheit, 32), 1.8)
            mean_max_temp, mean_min_temp = np.mean(rec[1]), np.mean(rec[2]) # Mean Max and Min Avg Temp calculated
            acc_precipit = 


# WeatherDataAPI().connect_server_and_ingest_data()
WeatherDataAPI().fetch_distinct_weather_records_and_calculate()

