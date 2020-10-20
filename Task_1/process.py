import argparse
import time
from dask import dataframe as dd
import pandas as pd


def main(args):
    if args['process']:
        start = time.time()
        data = pd.read_csv(args['filename'], usecols=[
                       'Unique Key', 'Agency Name', 'Complaint Type', 'Borough'])
        data.to_csv("out.csv", index=False,
                header=True, line_terminator='\n')
        end = time.perf_counter()
        print("Preprocessing time: " + "{:.2f}".format(time.time() - start) + " seconds")
        args['filename'] = "out.csv"
        columns = ["UniqueKey","AgencyName","ComplaintType","Borough"]
    else:
        columns =["UniqueKey","CreatedDate","ClosedDate","Agency","AgencyName","ComplaintType","Descriptor","LocationType","IncidentZip","IncidentAddress","StreetName","CrossStreet1","CrossStreet2","IntersectionStreet1","IntersectionStreet2","AddressType","City","Landmark","FacilityType","Status","DueDate","ResolutionDescription","ResolutionActionUpdatedDate","CommunityBoard","BBL","Borough","XCoordinate(StatePlane)","YCoordinate(StatePlane)","OpenDataChannelType","ParkFacilityName","ParkBorough","VehicleType","TaxiCompanyBorough","TaxiPickUpLocation","BridgeHighwayName","BridgeHighwayDirection","RoadRamp","BridgeHighwaySegment","Latitude","Longitude","Location"]

    dfd = dd.read_csv(
        args['filename'], 
        delimiter=',',
        names=columns,
        dtype='string',
        blocksize=128000000 # 128Mb chunks
    )

    if args['query'] == 0:
        daskjob = dfd.groupby('ComplaintType')['ComplaintType'].count().nlargest(10).compute()
    elif args['query'] == 1:
        daskjob = dfd.groupby(['ComplaintType', 'Borough']).count().reset_index().compute().sort_values(['Borough','UniqueKey'], ascending=[True, False]).drop_duplicates(subset='Borough')[['Borough', 'ComplaintType', 'UniqueKey']]
    elif args['query'] == 2:
        daskjob = dfd.groupby('AgencyName')['AgencyName'].count().nlargest(10).compute()
    else:
        daskjob = "Query number is not valid!"

    print(daskjob)


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-f", "--filename", type=str, default="311_Service_Requests_from_2010_to_Present.csv", help="FileName")
    ap.add_argument("-p", "--process", type=bool, default=False, help="Process Data")
    ap.add_argument("-q", "--query", type=int, default=0, help="Query")
    args = vars(ap.parse_args())
    start_time = time.time()
    main(args)
    print("{:.2f}".format(time.time() - start_time) + " seconds")
