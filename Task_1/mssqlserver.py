import pyodbc 
import argparse
import time
import pathlib


def create_table(cursor):
    cursor.execute("""
        IF OBJECT_ID('service_request', 'u') IS NOT NULL 
            DROP TABLE service_request;

        CREATE TABLE service_request (
            "Unique Key"                        nvarchar(max),
            "Created Date"                      nvarchar(max),
            "Closed Date"                       nvarchar(max),
            "Agency"                            nvarchar(max),
            "Agency Name"                       nvarchar(max),
            "Complaint Type"                    nvarchar(max),      
            "Descriptor"                        nvarchar(max),      
            "Location Type"                     nvarchar(max),
            "Incident Zip"                      nvarchar(max),
            "Incident Address"                  nvarchar(max),
            "Street Name"                       nvarchar(max),
            "Cross Street 1"                    nvarchar(max),
            "Cross Street 2"                    nvarchar(max),
            "Intersection Street 1"             nvarchar(max),
            "Intersection Street 2"             nvarchar(max),
            "Address Type"                      nvarchar(max),
            "City"                              nvarchar(max),
            "Landmark"                          nvarchar(max),
            "Facility Type"                     nvarchar(max),
            "Status"                            nvarchar(max),
            "Due Date"                          nvarchar(max),
            "Resolution Description"            nvarchar(max),
            "Resolution Action Updated Date"    nvarchar(max),
            "Community Board"                   nvarchar(max),
            "BBL"                               nvarchar(max),
            "Borough"                           nvarchar(max),
            "X Coordinate (State Plane)"        nvarchar(max),
            "Y Coordinate (State Plane)"        nvarchar(max),
            "Open Data Channel Type"            nvarchar(max),
            "Park Facility Name"                nvarchar(max),
            "Park Borough"                      nvarchar(max),
            "Vehicle Type"                      nvarchar(max),
            "Taxi Company Borough"              nvarchar(max),
            "Taxi Pick Up Location"             nvarchar(max),
            "Bridge Highway Name"               nvarchar(max),
            "Bridge Highway Direction"          nvarchar(max),
            "Road Ramp"                         nvarchar(max),
            "Bridge Highway Segment"            nvarchar(max),
            "Latitude"                          nvarchar(max),
            "Longitude"                         nvarchar(max),
            "Location"                          nvarchar(max)
        );
    """)


def import_csv(cursor, file_name):
        path = str(pathlib.Path(__file__).parent.absolute()).replace("\\", "\\\\") + "\\\\" + file_name
        cursor.execute(f"""BULK INSERT service_request FROM '{path}' WITH ( FORMAT='CSV', ROWTERMINATOR = '0x0A');""")


def execute_request(cursor, request):
    cursor.execute(request)
    for row in cursor:
        print(row)


def main(args):
    if args['query'] == 0:
        query = 'select top 10 "Complaint Type", count("Complaint Type") as ct from service_request group by "Complaint Type" order by ct desc'
    elif args['query'] == 1:
        query = 'with cte as (select "Borough", "Complaint Type", count("Complaint Type") as ct from service_request group by "Borough", "Complaint Type") SELECT "Borough", "Complaint Type", ct FROM ( SELECT "Borough", "Complaint Type", ct, ROW_NUMBER() OVER (PARTITION BY "Borough" ORDER BY ct desc) AS rn FROM cte) tmp WHERE rn = 1;'
    elif args['query'] == 2:
        query = 'select top 10 "Agency Name", count("Agency Name") as ct from service_request group by "Agency Name" order by ct desc'
    else:
        print("Query number is not valid!")
        return

    conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=DESKTOP-QS3OV80;'
                      'Database=BIGDATA1;'
                      'Trusted_Connection=yes;', autocommit = True)
    cursor = conn.cursor()

    if args['initialize']:
        start_time = time.time()
        create_table(cursor)
        import_csv(cursor, args['filename'])
        print("Initialization: " + "{:.2f}".format(time.time() - start_time) + " seconds")

    start_time = time.time()
    execute_request(cursor, query)
    print("Query: " + "{:.2f}".format(time.time() - start_time) + " seconds")


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-f", "--filename", type=str, default="311_Service_Requests_from_2010_to_Present.csv", help="FileName")
    ap.add_argument("-q", "--query", type=int, default=0, help="Query")
    ap.add_argument("-i", "--initialize", type=bool, default=False, help="Initialize Table in DataBase")
    args = vars(ap.parse_args())
    main(args)
