import os
import psycopg2

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="testing123"
)

# Create a cursor object
cur = conn.cursor()

# Loop through each file in the wx_data directory
for filename in os.listdir(os.path.join(os.getcwd(), "code-challenge-template", "wx_data")):
    if filename.endswith(".txt"):
        with open(os.path.join(os.getcwd(), "code-challenge-template", "wx_data", filename), "r") as f:
            # Get the station name from the filename
            station = filename.split(".")[0]

            # Initialize dictionaries to store data for each year
            max_temps = {}
            min_temps = {}
            precipitations = {}

            # Loop through each line in the file
            for line in f:
                # Split the line into its components
                date, max_temp, min_temp, precipitation = line.strip().split("\t")

                # Ignore missing data
                if max_temp == "-9999" or min_temp == "-9999" or precipitation == "-9999":
                    continue

                # Extract the year from the date
                year = int(date[:4])

                # Calculate statistics for this year
                if year not in max_temps:
                    max_temps[year] = []
                if year not in min_temps:
                    min_temps[year] = []
                if year not in precipitations:
                    precipitations[year] = []

                max_temps[year].append(float(max_temp) / 10)
                min_temps[year].append(float(min_temp) / 10)
                precipitations[year].append(float(precipitation) / 10)

            # Calculate statistics for each year and insert them into the database
            for year in max_temps:
                avg_max_temp = sum(max_temps[year]) / len(max_temps[year])
                avg_min_temp = sum(min_temps[year]) / len(min_temps[year])
                total_precipitation = sum(precipitations[year]) / 10

                cur.execute(
                    "INSERT INTO weather_statistics (year, station, avg_max_temperature, avg_min_temperature, total_precipitation) VALUES (%s, %s, %s, %s, %s)",
                    (year, station, avg_max_temp, avg_min_temp, total_precipitation))
            print("Data Inserted Successfully.")
            print(f'{cur.rowcount} row(s) inserted')
        # Commit changes to the database after processing each file
        conn.commit()

# Close the cursor and connection objects
cur.close()
conn.close()
