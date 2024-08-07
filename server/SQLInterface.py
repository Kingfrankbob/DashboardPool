import datetime
import json
import requests
import mysql.connector


class SQLInterface:
    def __init__(self):
        self.requests = requests
        self.data = None

    def update(self):
        self.fetchData()
        self.putData()  

    def fetchData(self):
        try:
            response = self.requests.get('https://sheets.googleapis.com/v4/spreadsheets/GITHUB_SCRET_SHEET_ID/values/DataLogger?key=GITHUB_SECRET_API_KEY')
            data = response.json()

            headers = data['values'][0]
            values = data['values'][1:]

            self.data = [dict(zip(headers, row)) for row in values]

        except:
            print('Error fetching data from google sheets')


    def putData(self):
        with open('/home/pooldbpi/Downloads/DashBoardPool-main/SQLConfig.json') as config_file:
            config = json.load(config_file)

        conn = mysql.connector.connect(
            host=config["host"],
            user=config["user"],
            password=config["password"],
            database=config["database"]
        )

        cursor = conn.cursor()

        cursor.execute('CREATE TABLE IF NOT EXISTS poolData (ID INTEGER AUTO_INCREMENT PRIMARY KEY, Tag TEXT, Air1 REAL, Air2 REAL, Pool1 REAL, Pool2 REAL, UncleanedTime TEXT, PumpOn TEXT, HeaterOn TEXT, SolarOn TEXT)')

        cursor.execute("SELECT MAX(STR_TO_DATE(UncleanedTime, '%m/%d/%Y %H:%i:%s')) FROM poolData")
        max_timestamp = cursor.fetchone()[0]

        for row in self.data:
            timestamp = datetime.datetime.strptime(row["UncleanedTime"], '%m/%d/%Y %H:%M:%S')


            if max_timestamp is None or timestamp > max_timestamp:
                air1 = None if row["Air1"] == 'null' else float(row["Air1"])
                air2 = None if row["Air2"] == 'null' else float(row["Air2"])
                pool1 = None if row["Pool1"] == 'null' else float(row["Pool1"])
                pool2 = None if row["Pool2"] == 'null' else float(row["Pool2"])
                values = (row["ID"], row["Tag"], air1, air2, pool1, pool2, row["UncleanedTime"], row["PumpOn"], row["HeaterOn"], row["SolarOn"])

                query = "INSERT INTO poolData (ID, Tag, Air1, Air2, Pool1, Pool2, UncleanedTime, PumpOn, HeaterOn, SolarOn) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                print("Inserting data for timestamp: " + row["UncleanedTime"])

                cursor.execute(query, values)

        conn.commit()
        print('Data inserted successfully')

    def sheets_have_id(self, id):
        try:
            response = self.requests.get('https://sheets.googleapis.com/v4/spreadsheets/GITHUB_SCRET_SHEET_ID/values/DataLogger?key=GITHUB_SECRET_API_KEY')
            data = response.json()

            headers = data['values'][0]
            values = data['values'][1:]

            data = [dict(zip(headers, row)) for row in values]

            for row in data:
                if row["ID"] == id:
                    return True
                    
        except:
            print('Error fetching data from google sheets')
            
        return False

    def insert_data(self, tag, air1, air2, pool1, pool2, pump_on, heater_on, solar_on):
        with open('/home/pooldbpi/Downloads/DashBoardPool-main/SQLConfig.json') as config_file:
            config = json.load(config_file)

        conn = mysql.connector.connect(
            host=config["host"],
            user=config["user"],
            password=config["password"],
            database=config["database"],
        )

        cursor = conn.cursor()

        cursor.execute("SELECT MAX(ID) FROM poolData")
        max_id = cursor.fetchone()[0]
        next_id = max_id + 1 if max_id is not None else 1

        if not self.sheets_have_id(next_id):
            requests.get(f'https://script.google.com/macros/s/{config["macro"]}/exec?tag={tag}&air1={air1}' + \
                f'&air2={air2}&pool1={pool1}&pool2={pool2}&pump_on={pump_on}&heater_on={heater_on}&solar_on={solar_on}')


        current_time = datetime.datetime.now().strftime('%m/%d/%Y %H:%M:%S')

        values = (next_id, tag, air1, air2, pool1, pool2, current_time, pump_on, heater_on, solar_on)
        query = "INSERT INTO poolData (ID, Tag, Air1, Air2, Pool1, Pool2, UncleanedTime, PumpOn, HeaterOn, SolarOn) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        cursor.execute(query, values)
        conn.commit()
        cursor.close()
        conn.close()

        print('New data inserted successfully')
  

def main():
    sql = SQLInterface()
    sql.update()

if __name__ == '__main__':
    main()
