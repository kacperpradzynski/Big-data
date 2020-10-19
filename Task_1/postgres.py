import psycopg2

from datetime import datetime

db_host = 'localhost'
db_database = 'big_data'
db_user = 'postgres'
db_password = 'postgres'

db_conn = psycopg2.connect(
    host=db_host,
    database=db_database,
    user=db_user,
    password=db_password
)


def create_table(db_cursor):
    print("Creating table")
    db_cursor.execute("""
        DROP TABLE IF EXISTS service_request;
        CREATE TABLE service_request (
            "Unique Key"                        TEXT,
            "Created Date"                      TEXT,
            "Closed Date"                       TEXT,
            "Agency"                            TEXT,
            "Agency Name"                       TEXT,
            "Complaint Type"                    TEXT,      
            "Descriptor"                        TEXT,      
            "Location Type"                     TEXT,
            "Incident Zip"                      TEXT,
            "Incident Address"                  TEXT,
            "Street Name"                       TEXT,
            "Cross Street 1"                    TEXT,
            "Cross Street 2"                    TEXT,
            "Intersection Street 1"             TEXT,
            "Intersection Street 2"             TEXT,
            "Address Type"                      TEXT,
            "City"                              TEXT,
            "Landmark"                          TEXT,
            "Facility Type"                     TEXT,
            "Status"                            TEXT,
            "Due Date"                          TEXT,
            "Resolution Description"            TEXT,
            "Resolution Action Updated Date"    TEXT,
            "Community Board"                   TEXT,
            "BBL"                               TEXT,
            "Borough"                           TEXT,
            "X Coordinate (State Plane)"        TEXT,
            "Y Coordinate (State Plane)"        TEXT,
            "Open Data Channel Type"            TEXT,
            "Park Facility Name"                TEXT,
            "Park Borough"                      TEXT,
            "Vehicle Type"                      TEXT,
            "Taxi Company Borough"              TEXT,
            "Taxi Pick Up Location"             TEXT,
            "Bridge Highway Name"               TEXT,
            "Bridge Highway Direction"          TEXT,
            "Road Ramp"                         TEXT,
            "Bridge Highway Segment"            TEXT,
            "Latitude"                          TEXT,
            "Longitude"                         TEXT,
            "Location"                          TEXT
        );
    """)
    db_conn.commit()
    print("Table created")


def import_csv(db_cursor, file_name):
    sqlcopy = "COPY service_request FROM STDIN DELIMITER ',' CSV HEADER"
    print("Starting data load")
    start_time = datetime.now()
    db_cursor.copy_expert(sqlcopy, open(file_name, 'r', encoding='utf8'))
    end_time = datetime.now()
    db_conn.commit()
    elapsed_time = (end_time - start_time)
    print("Data load eneded")
    return elapsed_time


def execute_request(db_cursor, request):
    print("Starting request: ", request)
    start_time = datetime.now()
    db_cursor.execute(request)
    records = db_cursor.fetchmany(10)
    end_time = datetime.now()
    elapsed_time = (end_time - start_time)
    print("Request ended")

    return elapsed_time, records


if __name__ == '__main__':

    print("Connecting to Database")
    db_cursor = db_conn.cursor()

    # create_table(db_cursor)

    # csv_file = '311_Service_Requests_from_2010_to_Present.csv'
    # elapsed_time = import_csv(db_cursor, csv_file)
    # print("Data loading time: ")
    # print(elapsed_time)

    request_1 = 'select "Complaint Type", count("Complaint Type") as ct from service_request group by "Complaint Type" order by ct desc'
    request_2 = 'select distinct on("Borough") "Borough", "Complaint Type", ct from(select "Borough", "Complaint Type", count("Complaint Type") as ct from service_request group by "Borough", "Complaint Type" order by ct desc) t1 order by "Borough", ct desc'
    request_3 = 'select "Agency", count("Complaint Type") as ct from service_request group by "Agency" order by ct desc'

    elapsed_time_1, records_1 = execute_request(db_cursor, request_1)
    print("Request time: ", elapsed_time_1)
    for row in records_1:
        print("Complaint Type: ", row[0], " | Quantity: ", row[1], "\n")

    elapsed_time_2, records_2 = execute_request(db_cursor, request_2)
    print("Request time: ", elapsed_time_2)
    for row in records_2:
        print("Borough: ", row[0], " | Complaint Type",
              row[1], " | Quantity: ", row[2], "\n")

    elapsed_time_3, records_3 = execute_request(db_cursor, request_3)
    print("Request time: ", elapsed_time_3)
    for row in records_3:
        print("Agency: ", row[0], " | Quantity: ", row[1], "\n")

    total_time = elapsed_time + elapsed_time_1 + elapsed_time_2+elapsed_time_3
    print("Total time: ", total_time)

    db_conn.close()
