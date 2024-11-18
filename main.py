import requests
import sqlite3

# Step 1: Fetch data from NASA API
url = "https://data.nasa.gov/resource/gh4g-9sfh.json"
response = requests.get(url)
meteorite_data = response.json()

# Define bounding boxes for each region
regions = {
    "Africa_MiddleEast_Meteorites": (-17.8, -35.2, 62.2, 37.6),
    "Europe_Meteorites": (-24.1, 38.0, 32.1, 71.1),
    "Upper_Asia_Meteorites": (33.0, 38.0, 190.4, 72.7),
    "Lower_Asia_Meteorites": (63.0, -9.9, 154.0, 37.6),
    "Australia_Meteorites": (112.9, -43.8, 154.3, -11.1),
    "North_America_Meteorites": (-168.2, 12.8, -52.0, 71.5),
    "South_America_Meteorites": (-81.2, -55.8, -34.4, 12.5),
}

# Step 2: Create SQLite database and tables for each region
conn = sqlite3.connect('meteorites.db')
cursor = conn.cursor()

for region in regions.keys():
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {region} (
            name TEXT,
            mass TEXT,
            reclat TEXT,
            reclong TEXT
        )
    ''')
conn.commit()

# Step 3: Filter data and insert into corresponding tables
def is_in_region(lat, long, bounding_box):
    min_long, min_lat, max_long, max_lat = bounding_box
    return min_long <= long <= max_long and min_lat <= lat <= max_lat

for entry in meteorite_data:
    # Only proceed if reclat and reclong fields are present
    if 'reclat' in entry and 'reclong' in entry:
        try:
            lat = float(entry['reclat'])
            long = float(entry['reclong'])
            name = entry.get('name', 'Unknown')
            mass = entry.get('mass', 'Unknown')

            # Insert entry into the appropriate region table
            for region, bounding_box in regions.items():
                if is_in_region(lat, long, bounding_box):
                    cursor.execute(f'''
                        INSERT INTO {region} (name, mass, reclat, reclong)
                        VALUES (?, ?, ?, ?)
                    ''', (name, mass, entry['reclat'], entry['reclong']))
                    break
        except ValueError:
            # Skip entries where reclat or reclong cannot be converted to float
            continue

conn.commit()
conn.close()