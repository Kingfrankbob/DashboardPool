import json
from flask import Flask, jsonify
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