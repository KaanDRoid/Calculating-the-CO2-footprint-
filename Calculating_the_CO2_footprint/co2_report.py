import duckdb
import sys
import pandas as pd
from haversine import haversine, Unit

ROUTE_TO_COORDINATES = {
    112: ((40.7128, -74.0060), (37.7749, -122.4194)),  # Example: NYC to San Francisco
    150: ((51.5074, -0.1278), (48.8566, 2.3522)), # London to Paris
    138: ((37.7749, -122.4194), (34.0522, -118.2437)), # San Francisco to LA
    169: ((48.8566, 2.3522), (35.6895, 139.6917)), # Paris to Tokyo
    197: ((52.5200, 13.4050), (40.7128, -74.0060)), # Berlin to NYC
    146: ((35.6895, 139.6917), (37.7749, -122.4194)), # Tokyo to San Francisco
    181: ((40.7128, -74.0060), (51.5074, -0.1278)), # NYC to London
    185: ((37.7749, -122.4194), (52.5200, 13.4050)), # San Francisco to Berlin
    189: ((51.5074, -0.1278), (35.6895, 139.6917)), # London to Tokyo
    155: ((48.8566, 2.3522), (34.0522, -118.2437)), # Paris to LA
}

def calculate_co2_footprint(route_index):
    """
    Calculate the CO2 footprint for a given route index using the Haversine formula.
    """
    if route_index not in ROUTE_TO_COORDINATES:
        return 0  # Return 0 if the route index is not mapped

    # Get the coordinates for the route
    start_coords, end_coords = ROUTE_TO_COORDINATES[route_index]
    # Calculate distance in kilometers
    distance_km = haversine(start_coords, end_coords, unit=Unit.KILOMETERS)
    # Calculate CO2 footprint (0.1 kg CO2 per km)
    return distance_km * 0.1

def query_data(db_connection, date_from, date_to):
    """
    Query the database for flight records between the given dates.
    """
    query = f'''
        SELECT department, route_index
        FROM company_flights
        WHERE flight_date BETWEEN '{date_from}' AND '{date_to}'
    '''
    return db_connection.execute(query).fetchall()

def generate_report(data, date_from, date_to):
    """
    Generate a CO2 footprint report by department.
    """
    df = pd.DataFrame(data, columns=['department', 'route_index'])
    df['co2_footprint'] = df['route_index'].apply(calculate_co2_footprint)
    report = df.groupby('department')['co2_footprint'].sum().reset_index()
    output_file = f"co2_report_{date_from}_{date_to}.csv"
    report.to_csv(output_file, index=False)
    print(f"CO2 report generated: {output_file}")

def main(date_from, date_to):
    conn = duckdb.connect(database='C:/Users/kaana/OneDrive/Masaüstü/Big Data/Data Science/Calculating_the_CO2_footprint/flights.duckdb', read_only=True)
    data = query_data(conn, date_from, date_to)
    if not data:
        print("No flights found in the specified date range.")
    else:
        generate_report(data, date_from, date_to)
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python co2_report.py DATE_FROM DATE_TO")
        sys.exit(1)
    date_from = sys.argv[1]
    date_to = sys.argv[2]
    main(date_from, date_to)