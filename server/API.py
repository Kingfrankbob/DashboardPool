import json
from flask import Flask, jsonify, request
import mysql.connector
from SQLInterface import SQLInterface

app = Flask(__name__)
# http://localhost:5000/api/allPoolData

# Gets all the data from the database
@app.route('/api/allPoolData', methods=['GET'])
def get_pool_data():
    update_database()
    conn = connect_to_database()

    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT * FROM poolData")
    rows = cursor.fetchall()

    return jsonify(rows)

# Gets the last year of data from the database
@app.route('/api/yearPoolData', methods=['GET'])
def get_year_pool_data():
    update_database()
    conn = connect_to_database()

    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT * FROM poolData WHERE STR_TO_DATE(UncleanedTime, '%m/%d/%Y %H:%i:%s') > DATE_SUB(NOW(), INTERVAL 1 YEAR)")
    rows = cursor.fetchall()

    return jsonify(rows)

# Gets the last month of data from the database
@app.route('/api/monthPoolData', methods=['GET'])
def get_month_pool_data():
    update_database()
    conn = connect_to_database()

    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT * FROM poolData WHERE STR_TO_DATE(UncleanedTime, '%m/%d/%Y %H:%i:%s') > DATE_SUB(NOW(), INTERVAL 1 MONTH)")
    rows = cursor.fetchall()

    return jsonify(rows)

# Gets the last week of data from the database
@app.route('/api/weekPoolData', methods=['GET'])
def get_week_pool_data():
    update_database()
    conn = connect_to_database()

    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT * FROM poolData WHERE STR_TO_DATE(UncleanedTime, '%m/%d/%Y %H:%i:%s') > DATE_SUB(NOW(), INTERVAL 1 WEEK)")
    rows = cursor.fetchall()

    return jsonify(rows)

# Gets the last day of data from the database
@app.route('/api/dayPoolData', methods=['GET'])
def get_day_pool_data():
    update_database()
    conn = connect_to_database()

    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT * FROM poolData WHERE STR_TO_DATE(UncleanedTime, '%m/%d/%Y %H:%i:%s') > DATE_SUB(NOW(), INTERVAL 1 DAY)")
    rows = cursor.fetchall()

    return jsonify(rows)

# Gets the last hour of data from the database
@app.route('/api/hourPoolData', methods=['GET'])
def get_hour_pool_data():
    update_database()
    conn = connect_to_database()

    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT * FROM poolData WHERE STR_TO_DATE(UncleanedTime, '%m/%d/%Y %H:%i:%s') > DATE_SUB(NOW(), INTERVAL 1 HOUR)")
    rows = cursor.fetchall()

    return jsonify(rows)

# Get the average temperature of the pool for all time by week
# Breaks the data into weeks and averages the pool temperature for each week
@app.route('/api/averagePoolTempByWeek', methods=['GET'])
def get_average_pool_temp_by_week():
    update_database()
    conn = connect_to_database()

    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT AVG(Pool1) as avgPoolTemp, WEEK(STR_TO_DATE(UncleanedTime, '%m/%d/%Y %H:%i:%s')) as week FROM poolData GROUP BY week")
    rows = cursor.fetchall()

    return jsonify(rows)

@app.route('/api/insertValue', methods=['POST'])
def insert_value():
    try:
        data = request.get_json()
        tag = data.get('tag')
        air1 = data.get('air1')
        air2 = data.get('air2')
        pool1 = data.get('pool1')
        pool2 = data.get('pool2')
        pump_on = data.get('pump_on')
        heater_on = data.get('heater_on')
        solar_on = data.get('solar_on')

        sql = SQLInterface()
        sql.insert_data(tag, air1, air2, pool1, pool2, pump_on, heater_on, solar_on)

        return jsonify({'message': 'Data inserted successfully'}), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Helpers
# Update the data in the database
def update_database():
    sql = SQLInterface()
    sql.update()


def connect_to_database():
    with open('server/SQLConfig.json') as config_file:
        config = json.load(config_file)

    conn = mysql.connector.connect(
        host=config["host"],
        user=config["user"],
        password=config["password"],
        database=config["database"]
    )

    return conn


if __name__ == '__main__':
    app.run(debug=True)