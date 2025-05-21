# Routes/demandRoute.py
from flask import Blueprint, jsonify, request
from datetime import datetime
import mysql.connector
from mysql.connector import Error
import json  # Import json module to dump JSON data as string

demand_bp = Blueprint('demand_bp', __name__)

# MySQL database connection configuration
db_config = {
    'user': 'root',
    'password': '',
    'host': 'localhost',
    'database': 'guvnl_dev'
}

