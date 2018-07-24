import tempfile
import os
import requests
from pyclowder.datasets import DatasetsApi

"""
The sensor_name example provided below will download only one dataset.

Be careful changing the sensor name e.g. to a Season 4 plot, you might end up downloading
a lot more data than you intend. Consider including ?since=YYYY-MM-DD&until=YYYY-MM-DD to the
/datapoints query to reduce number of datasets returned.
"""

root_url = "https://terraref.ncsa.illinois.edu/clowder/"
api_key = "dac601c0-bdfb-4ad1-9e46-4502db5694f7"
sensor_name = " "
stream_name = 'Thermal IR GeoTIFFs Datasets'

output_path = "/Users/sathp/Desktop/ncsa-research/retraining/images/"

def fetch(sensor_nam, start_date, end_date):

    # Setting sensor_name
    sensor_name = sensor_nam

    # Get sensor id by name
    sens_url = root_url+"api/geostreams/sensors?sensor_name=%s" % sensor_name
    r = requests.get(sens_url)
    if r.status_code == 200:
        sensor_id = r.json()[0]['id']
        print("SENSOR [%s]: id %s" % (sensor_name, sensor_id))
    else:
        print("sensor not found")
        exit()

    # Get stream id by name
    strm_url = root_url+"api/geostreams/sensors/%s/streams" % sensor_id
    r = requests.get(strm_url)
    stream_id = None
    if r.status_code == 200:
        for strm_obj in r.json():
            if strm_obj['name'].startswith(stream_name):
                stream_id = strm_obj['stream_id']
                print("STREAM [%s]: id %s" % (strm_obj['name'], stream_id))
    if not stream_id:
        print("stream not found")
        exit()

    # Get datapoints by stream
    dp_url = root_url+"api/geostreams/datapoints?stream_id=%s&since=%s&until=%s" % (stream_id, start_date, end_date)
    r = requests.get(dp_url)
    if r.status_code == 200:
        dp_list = r.json()
        print("found %s datapoints" % len(dp_list))
        for datapoint in dp_list:
            props = datapoint['properties']
            if 'source_dataset' in props:
                # Get dataset ID from the datapoint source and download to temporary zip file
                ds_id = props['source_dataset'].split("/")[-1]
                #ds_url = props['source_dataset'].replace("/datasets", "/api/datasets") + "files"
                zipfile = download_dataset(root_url, api_key, ds_id, props["dataset_name"], sensor_name)
                print("%s: %s" % (props["dataset_name"], zipfile))
    else:
        print(str(r.json))

def download_dataset(host, key, datasetid, dataset_name, sensor_name):
    # fetch dataset zipfile and download to temp directory
    url = '%sapi/datasets/%s/download?key=%s' % (host, datasetid, key)
    result = requests.get(url, stream=True, verify=True)
    result.raise_for_status()

    filename = sensor_name.replace(" ", "").replace("MACFieldScanner", "").replace("Season", "S").replace("Range", "R").replace("Column", "C")+"_"+str(dataset_name).replace(" ", "").replace("ThermalIRGeoTIFFs", "TIR")
    (filedescriptor, zipfile) = tempfile.mkstemp(suffix=".zip", prefix = filename, dir="/Users/sathp/Desktop/ncsa-research/retraining/images/")
    with os.fdopen(filedescriptor, "w") as outfile:
        for chunk in result.iter_content(chunk_size=10 * 1024):
            outfile.write(chunk)
 
    return zipfile