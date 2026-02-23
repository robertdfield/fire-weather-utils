"""
Script to query local parq file:

For each fire that is active in the input bbox
and between the queried dates, output its fireid, mergeid, start time,
end or latest time, end or latest area, and current centroid.

This works on the 'parq' 'all fires'', i.e. including fireIDs before they may have been merged into a different 'mergeid'

I am currently assuming that 'mergeid' is the final mergeid, not an intermediate mergeid

This should eventually be combined with 'get_largefire_centroids_local.py'

It also doesn't have the 'region' column that largefire output has because it was not in the 'parq' file.
"""

import datetime as dt
import pandas as pd
import geopandas as gpd
import pyarrow as pyar
import os.path
import csv

# Machine-specific main directory

# This is the 'brewer' partition of the 'dansgaard' RAID mounted on the 'hammer' machine
# MACHINEROOT = "/autofs/brewer/rfield1/storage/observations/FIRMS/VIIRS/FEDS/LOCAL/"

# laptop
MACHINEROOT = "/Users/rfield1/data/observations/FEDS/LOCAL/"

# Path to local parq file
FEDSDATANAME = "allfires_20251231_PM"
FEDSFILETYPE = "parq"
DOWNLOADED_FILEPATH =  MACHINEROOT + FEDSDATANAME + "." + FEDSFILETYPE

# start date for query, inclusive
START = "2025-06-01T00:00:00+00:00"

# end date for query, exclusive
STOP = "2025-09-30T23:59:00+00:00"

# output location and prefix
OUT_DIR = MACHINEROOT + "OUTPUT/" + FEDSDATANAME + "/"
OUT_FILE_PREFIX = "Centroids"

# assumes WGS84
# BBOX = ["-126.4", "24.0", "-61.4", "49.4"] # CONUS, roughly

# haven't tried this filter yet
BBOX_NAME = "IberianPeninsula" 
BBOX = [-10,35,5,44] #["-180", "-90", "180", "90"] 

##########################################################################################
#
# All settings above here please
#
##########################################################################################
if not os.path.exists(OUT_DIR):
   os.mkdir(OUT_DIR)   

# this used to be the 'largefire' fgb file
df = gpd.read_parquet(DOWNLOADED_FILEPATH)

print("Before reset_index")
df.info(verbose=True)

# this turns 'fireID' into a regular column, instead of an 'index' (like a row-type dataframe index that I don't really understand)
print("After reset_index")
df = df.reset_index()
df.info(verbose=True)

rows, columns = df.shape
print(f"Number of rows: {rows}, Number of columns: {columns}")

df.t = pd.to_datetime(df.t, utc=True)
start = pd.to_datetime(START)
stop = pd.to_datetime(STOP)

#if BBOX:
#    df = df.cx[BBOX[0] : BBOX[2], BBOX[1] : BBOX[3]]

# get ids of fires active in timestep
ids = df[(df["t"] >= start) & (df["t"] <= stop)].fireID.unique()
print(f"Number of unique IDs in time range: {len(ids)}")

# get all timesteps for fires that were active in date range even if some timesteps are outside of date range
# note that unlike with API version, this could lead to having dates after the STOP param
filtered = df[df["fireID"].isin(ids)]
filtered.reset_index()

fires = []

#fireIDList = [89637,91160]
fireIDList = filtered.fireID.unique()

print(f"Processing {rows} fires")
for fireID in fireIDList:
    rows = filtered[filtered["fireID"] == fireID].sort_values(by="t", ascending=False)
    
    # check to see that the fireID is associated with only 1 mergeid
    listOfMergeID = rows.mergeid.unique()
    if len(listOfMergeID) != 1:
        print('{fireID} has not 1 mergeid')
        sys.exit()
    fire = {
        "fireid": fireID,
        "mergeid": listOfMergeID[0],
        "start_t": rows.t.min(),
        "latest_t": rows.t.max(),
        "max_farea": rows.farea.max(),
        "centroid": rows.hull.centroid.iloc[0],  # of first
# Not seeing 'region' in 'allfires', which there was in 'largefires'
# I'm guessing this is appended in FEDS post-processing that makes 'largefires', after the 'parq' file gets written
# So omitting for now
#        "region": rows.region.iloc[0],
    }
    fires.append(fire)

print(f"Processed {len(fires)} fires")
fires = gpd.GeoDataFrame.from_dict(fires, geometry="centroid", crs=filtered.crs)
fires = fires.to_crs(crs="epsg:4326")

# filter to fires in BBOX...should really be doing this before the main for loop!!!
areFiresInBox = (fires['centroid'].x>=BBOX[0]) & (fires['centroid'].x<=BBOX[2]) & (fires['centroid'].y>=BBOX[1]) & (fires['centroid'].y<=BBOX[3])
firesToWrite = fires[areFiresInBox]

start_str = dt.datetime.fromisoformat(START).strftime("%Y%m%d")
stop_str = dt.datetime.fromisoformat(STOP).strftime("%Y%m%d")
outpath = OUT_DIR + OUT_FILE_PREFIX + "_" + BBOX_NAME + "_" + start_str + "_" + stop_str + ".csv"
firesToWrite.to_csv(outpath, index=False)

print(f"Wrote {len(fires)} fires to {outpath}")
