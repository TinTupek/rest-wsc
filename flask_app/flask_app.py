from flask import Flask, jsonify, make_response, request
import psycopg2
import os
import csv
import io
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.route('/api', methods=['GET'], strict_slashes=False)
def save_weather_data():
    # Retrieve and parse query parameters
    city_names = request.args.getlist('city')  # Allows multiple cities
    temp_unit = request.args.get('unit', 'C')  # Default temperature unit is Celsius
    response_format = request.args.get('format', 'json')  # Default format is JSON

    # Validate input parameters
    if not city_names or not any(city_names):  # Ensure at least one city name is provided
        logger.warning("No city names provided in the request.")
        return make_response(
            jsonify({"error": "At least one city name is required"}), 
            400
        )

    if response_format not in {'json', 'csv'}:
        logger.warning(f"Unsupported response format: {response_format}")
        return make_response(
            jsonify({"error": "Invalid format. Supported formats are 'json' and 'csv'."}),
            400
        )

    # Use DATABASE_URL from environment variables
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        logger.error("DATABASE_URL environment variable not set.")
        return make_response(
            jsonify({"error": "Database configuration missing"}), 
            500
        )

    try:
        # Establish a database connection
        conn = psycopg2.connect(database_url)
        cur = conn.cursor()

        query = "SELECT * FROM public.weather_data(%s, %s, %s)"
        params = (city_names, temp_unit, response_format)
        formatted_query = cur.mogrify(query, params).decode('utf-8')
        logger.info(f"Executing query: {formatted_query}")

        # Execute the query
        cur.execute(query, params)

        # Get the column names
        column_names = [desc[0] for desc in cur.description]

        # Check if 'batch_id' and 'status_code' columns exist
        batch_id_index = column_names.index('batch_id') 
        status_index = column_names.index('status_code') 
        format_index = column_names.index('file_type')

        # Fetch all data
        weather_data = cur.fetchall()

        # Extract batch_id and status from the first row if they exist
        if weather_data:
            first_row = weather_data[0]
            status = first_row[status_index] if status_index is not None else 500
        else:
            logger.warning("No weather data returned.")
            return make_response(
                jsonify({"error": "No weather data available for the requested cities."}), 
                404
            )

        # Remove batch_id, status_code, and file_type columns from the data
        excluded_indices = {i for i in (batch_id_index, status_index, format_index) if i is not None}
        filtered_weather_data = [
            tuple(value for i, value in enumerate(row) if i not in excluded_indices)
            for row in weather_data
        ]
        filtered_column_names = [
            name for i, name in enumerate(column_names) if i not in excluded_indices
        ]

        conn.commit()
        logger.info(f"Database transaction committed. HTTP Status: {status}")

        if response_format == 'csv':
            # Generate the CSV response
            csv_response = generate_csv_response(filtered_weather_data, filtered_column_names)
            response = make_response(csv_response)
            response.status_code = status
            return response
        else:
            response = make_response(jsonify(filtered_weather_data))
            response.status_code = status
            return response

    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        return make_response(
            jsonify({"error": "Database execution failed", "details": str(e)}), 
            500
        )

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return make_response(
            jsonify({"error": "An unexpected error occurred", "details": str(e)}), 
            500
        )

    finally:
        if 'conn' in locals() and conn is not None:
            conn.close()
            logger.info("Database connection closed.")

def generate_csv_response(data, columns):
    """Generate a CSV response from data and column headers."""
    try:
        stream = io.StringIO()
        writer = csv.writer(stream)
        writer.writerow(columns)  # Column headers
        writer.writerows(data)  # Data rows
        logger.info("CSV response generated successfully.")
        return make_response(stream.getvalue(), 200, {'Content-Type': 'text/csv'})
    except Exception as e:
        logger.error(f"Error generating CSV response: {e}")
        raise

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
