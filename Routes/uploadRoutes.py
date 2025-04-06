# iexRoutes.py
from flask import Blueprint, jsonify, request
import mysql.connector

# Create a Blueprint
uploadApi = Blueprint('upload', __name__)

# MySQL configuration
db_config = {
    'user': 'admin',
    'password': 'Babai123',
    'host': 'guvnl-dev.cv4e0cyc8rtk.ap-south-1.rds.amazonaws.com',
    'database': 'guvnl_dev'
}
# iexRoutes.py
from flask import Blueprint, jsonify, request
import mysql.connector

# Create a Blueprint
uploadApi = Blueprint('upload', __name__)

# MySQL configuration
db_config = {
    'user': 'admin',
    'password': 'Babai123',
    'host': 'guvnl-dev.cv4e0cyc8rtk.ap-south-1.rds.amazonaws.com',
    'database': 'guvnl_dev'
}


@uploadApi.route('/upload', methods=['POST'])
def upload_data():
    # Get JSON payload from request
    # {
    #     "tableName": "my_table",
    #     "columns": {
    #         "id": "INT",
    #         "name": "VARCHAR(255)",
    #         "age": "INT"
    #     },
    #     "data": [
    #         {"id": 1, "name": "Alice", "age": 30},
    #         {"id": 2, "name": "Bob", "age": 25}
    #     ]
    # }
    payload = request.get_json()
    if not payload:
        return jsonify({"error": "No input data provided."}), 400

    # Extract required fields
    table_name = payload.get('tableName')
    columns = payload.get('columns')  # Expected as dict, e.g., {"id": "INT", "name": "VARCHAR(255)"}
    rows = payload.get('data')  # Expected as list of dicts

    if not table_name or not columns or rows is None:
        return jsonify({"error": "Please provide 'tableName', 'columns' and 'data' in the payload."}), 400

    # Connect to the MySQL database
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
    except Exception as e:
        return jsonify({"error": f"Database connection failed: {str(e)}"}), 500

    try:
        # Check if table exists by querying information_schema
        table_exists_query = """
            SELECT COUNT(*)
            FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = %s
        """
        cursor.execute(table_exists_query, (db_config['database'], table_name))
        table_exists = cursor.fetchone()[0] == 1

        if table_exists:
            # If table exists, fetch its columns and types from information_schema
            table_cols_query = """
                SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH 
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
            """
            cursor.execute(table_cols_query, (db_config['database'], table_name))
            table_columns = cursor.fetchall()

            # Build a dictionary of {column_name: data_type_info} from the DB
            # For character types, include the maximum length (e.g., VARCHAR(255))
            table_columns_dict = {}
            for col_name, data_type, char_max in table_columns:
                if char_max:
                    # Note: This is a simple way to show type with length.
                    table_columns_dict[col_name] = f"{data_type.upper()}({char_max})"
                else:
                    table_columns_dict[col_name] = data_type.upper()

            # Compare each provided column against the DB definition
            for col, provided_dtype in columns.items():
                if col not in table_columns_dict:
                    return jsonify({"error": f"Column '{col}' does not exist in table '{table_name}'."}), 400

                # Normalize: compare only the base type (ignoring size)
                base_provided = provided_dtype.upper().split('(')[0].strip()
                base_actual = table_columns_dict[col].upper().split('(')[0].strip()

                if base_provided != base_actual:
                    return jsonify({
                        "error": (
                            f"Data type mismatch for column '{col}': "
                            f"provided '{provided_dtype.upper()}', actual '{table_columns_dict[col]}'"
                        )
                    }), 400

        else:
            # If table does not exist, create it using the provided column definitions.
            col_defs = []
            for col, dtype in columns.items():
                # We wrap the column name in backticks to handle reserved words etc.
                col_defs.append(f"`{col}` {dtype}")
            create_query = f"CREATE TABLE `{table_name}` ({', '.join(col_defs)})"
            try:
                cursor.execute(create_query)
                conn.commit()
            except Exception as e:
                return jsonify({"error": f"Error creating table: {str(e)}"}), 500

        # Now prepare to insert the data rows.
        # Use the provided columns as the order for insertion.
        col_names = list(columns.keys())
        # Build the INSERT statement with parameter placeholders.
        placeholders = ", ".join(["%s"] * len(col_names))
        cols_formatted = ", ".join([f"`{col}`" for col in col_names])
        insert_query = f"INSERT INTO `{table_name}` ({cols_formatted}) VALUES ({placeholders})"

        # Prepare values for each row
        values_to_insert = []
        for row in rows:
            # Ensure that the row contains every required column.
            row_values = []
            for col in col_names:
                if col not in row:
                    return jsonify({"error": f"Missing column '{col}' in one of the data rows."}), 400
                row_values.append(row[col])
            values_to_insert.append(tuple(row_values))

        # Bulk insert all rows
        cursor.executemany(insert_query, values_to_insert)
        conn.commit()

    except Exception as e:
        conn.rollback()
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

    finally:
        cursor.close()
        conn.close()

    return jsonify({"message": "Data uploaded successfully."}), 200


@uploadApi.route('/dbInfo', methods=['GET'])
def get_db_info():
    """
    This endpoint returns the total number of tables in the database and
    the schema details (column name, data type, maximum length, nullability,
    and default value) for each table.
    """
    try:
        # Connect to the MySQL database
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Query to count the number of tables in the current database
        count_query = """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = %s
        """
        cursor.execute(count_query, (db_config['database'],))
        table_count = cursor.fetchone()[0]

        # Query to get the list of table names
        tables_query = """
            SELECT TABLE_NAME
            FROM information_schema.tables
            WHERE table_schema = %s
        """
        cursor.execute(tables_query, (db_config['database'],))
        table_names = [row[0] for row in cursor.fetchall()]

        # For each table, retrieve its schema details
        table_schemas = {}
        for table in table_names:
            schema_query = """
                SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE, COLUMN_DEFAULT
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ORDINAL_POSITION
            """
            cursor.execute(schema_query, (db_config['database'], table))
            columns = cursor.fetchall()
            # Convert the column info into a list of dictionaries for better readability
            columns_list = []
            for column in columns:
                col_info = {
                    "column_name": column[0],
                    "data_type": column[1],
                    "max_length": column[2],
                    "is_nullable": column[3],
                    "default": column[4]
                }
                columns_list.append(col_info)
            table_schemas[table] = columns_list

        # Construct the response JSON
        response = {
            "number_of_tables": table_count,
            "tables": table_schemas
        }
        return jsonify(response), 200

    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

    finally:
        # Ensure that the database connection is properly closed
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
