from idlelib.query import Query

from db import get_db_connection
import psycopg2

def normalize_db():
    source_conn = get_db_connection()
    target_conn = psycopg2.connect(
        dbname="normal_db",
        user="shalom_bergman",
        password="1234",
        host="localhost",
        port="5432"
    )

    try:
        query1 = """
        CREATE TABLE IF NOT EXISTS target (
            target_id SERIAL PRIMARY KEY,
            target_country VARCHAR(100),
            target_city VARCHAR(100),
            target_type VARCHAR(100),
            target_industry VARCHAR(255),
            target_priority VARCHAR(5),
            target_latitude NUMERIC(10, 6),
            target_longitude NUMERIC(10, 6)
            );

        """
        query2 = """
        CREATE TABLE IF NOT EXISTS mission_normalized (
        mission_id SERIAL PRIMARY KEY,
        mission_date DATE,
        theater_of_operations VARCHAR(100),
        country VARCHAR(100),
        air_force VARCHAR(100),
        unit_id VARCHAR(100),
        aircraft_series VARCHAR(100),
        callsign VARCHAR(100),
        mission_type VARCHAR(100),
        takeoff_base VARCHAR(255),
        takeoff_location VARCHAR(255),
        takeoff_latitude NUMERIC(10, 6),
        takeoff_longitude NUMERIC(10, 6),
        target_id INTEGER REFERENCES target (target_id),  
        altitude_hundreds_of_feet NUMERIC(7, 2),
        airborne_aircraft NUMERIC(4, 1),
        attacking_aircraft INTEGER,
        bombing_aircraft INTEGER,
        aircraft_returned INTEGER,
        aircraft_failed INTEGER,
        aircraft_damaged INTEGER,
        aircraft_lost INTEGER,
        high_explosives VARCHAR(255),
        high_explosives_type VARCHAR(255),
        high_explosives_weight_pounds VARCHAR(25),
        high_explosives_weight_tons NUMERIC(10, 2),
        incendiary_devices VARCHAR(255),
        incendiary_devices_type VARCHAR(255),
        incendiary_devices_weight_pounds NUMERIC(10, 2),
        incendiary_devices_weight_tons NUMERIC(10, 2),
        fragmentation_devices VARCHAR(255),
        fragmentation_devices_type VARCHAR(255),
        fragmentation_devices_weight_pounds NUMERIC(10, 2),
        fragmentation_devices_weight_tons NUMERIC(10, 2),
        total_weight_pounds NUMERIC(10, 2),
        total_weight_tons NUMERIC(10, 2),
        time_over_target VARCHAR(8),
        bomb_damage_assessment VARCHAR(255),
        source_id VARCHAR(100)
            );
        """


        # execute the queries
        cur = target_conn.cursor()
        cur.execute(query1)
        cur.execute(query2)
        target_conn.commit()

        s_cur = source_conn.cursor()
        s_cur.execute("""
         SELECT DISTINCT target_country, target_city, target_type, target_industry, target_priority, target_latitude, target_longitude, 
                        mission_date, theater_of_operations, country, air_force, unit_id, aircraft_series, callsign, 
                        mission_type, takeoff_base, takeoff_location, takeoff_latitude, takeoff_longitude, altitude_hundreds_of_feet, 
                        airborne_aircraft, attacking_aircraft, bombing_aircraft, aircraft_returned, aircraft_failed, aircraft_damaged, 
                        aircraft_lost, high_explosives, high_explosives_type, high_explosives_weight_pounds, high_explosives_weight_tons, 
                        incendiary_devices, incendiary_devices_type, incendiary_devices_weight_pounds, incendiary_devices_weight_tons, 
                        fragmentation_devices, fragmentation_devices_type, fragmentation_devices_weight_pounds, fragmentation_devices_weight_tons, 
                        total_weight_pounds, total_weight_tons, time_over_target, bomb_damage_assessment, source_id
        FROM mission;        
        """)
        while True:
            mission_row = s_cur.fetchone()
            if mission_row is None:
                break

                target_country = mission_row[0]
                target_city = mission_row[1]
                target_type = mission_row[2]
                target_industry = mission_row[3]
                target_priority = mission_row[4]
                target_latitude = mission_row[5]
                target_longitude = mission_row[6]


                cur.execute("INSERT INTO target (target_country, target_city, target_type, target_industry, target_priority, target_latitude,target_longitude) "
                            "VALUES (%s, %s, %s,%s,%s,%s,%s) RETURNING target_id",
                            (target_country, target_city, target_type, target_industry, target_priority, target_latitude, target_longitude))
                target_id = cur.fetchone()[0]
                cur.execute("""
                                INSERT INTO mission_normalized ( mission_date, theater_of_operations, country, air_force, unit_id, aircraft_series, callsign,
                                                                mission_type, takeoff_base, takeoff_location, takeoff_latitude, takeoff_longitude, target_id,
                                                                altitude_hundreds_of_feet, airborne_aircraft, attacking_aircraft, bombing_aircraft, aircraft_returned, 
                                                                aircraft_failed, aircraft_damaged, aircraft_lost, high_explosives, high_explosives_type, 
                                                                high_explosives_weight_pounds, high_explosives_weight_tons, incendiary_devices, incendiary_devices_type, 
                                                                incendiary_devices_weight_pounds, incendiary_devices_weight_tons, fragmentation_devices, 
                                                                fragmentation_devices_type, fragmentation_devices_weight_pounds, fragmentation_devices_weight_tons, 
                                                                total_weight_pounds, total_weight_tons, time_over_target, bomb_damage_assessment, source_id)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
                            """, (
                mission_row[7], mission_row[8], mission_row[9], mission_row[10], mission_row[11], mission_row[12],
                mission_row[13], mission_row[14],
                mission_row[15], mission_row[16], mission_row[17], mission_row[18], mission_row[19], target_id,
                mission_row[20], mission_row[21],
                mission_row[22], mission_row[23], mission_row[24], mission_row[25], mission_row[26], mission_row[27],
                mission_row[28], mission_row[29],
                mission_row[30], mission_row[31], mission_row[32], mission_row[33], mission_row[34], mission_row[35],
                mission_row[36], mission_row[37],
                mission_row[38], mission_row[39], mission_row[40], mission_row[41], mission_row[42]))

        target_conn.commit()

    except Exception as e:
        print(e)
        return False
    finally:
        source_conn.close()
        target_conn.close()

