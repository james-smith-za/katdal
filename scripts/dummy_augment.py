# import numpy as np
import h5py
import sys

h5file = h5py.File(sys.argv[1], "r+")

h5file.attrs["version"] = "4.0"

h5file.create_group("Metadata")
h5file["Metadata"].create_group("Configuration")
h5file["Metadata/Configuration"].create_group("Observation")
obsAttrs = { "ants":"ant1", \
             "script_arguments":"augmented with dummy data", \
             "description":"Noise generator data", \
             "endtime":"unix time when the observation finished.", \
             "experiment_id":"YYYYMMDD-0001", \
             "script_name":"/home/kat/scripts/observation/example.py", \
             "nd_params":"Diode=coupler, On=10 s, Off=10 s, Period=180 s", \
             "observer":"Charles Copley", \
             "rf_params":"Centre freq=200 MHz, Dump rate=125 Hz", \
             "starttime":"Unix time when observation started.", \
             "status":"something" \
             }
for obsAttr in obsAttrs.iteritems():
    h5file["Metadata/Configuration/Observation"].attrs[obsAttr[0]] = obsAttr[1]

# h5file["Metadata"].create_group("Sensors")

# h5file.create_group("Markup")
# h5file.create_group("History")

def iterate_through_object(h5object, level=0):
    for item in h5object.iteritems():
        if type(item[1]) == h5py.Group:
            print "%sGroup: %s" % ("  " * level + "|-", item[0])
            iterate_through_object(item[1], level + 1)
        else:
            print "%sDataset: %s" % ("  " * (level) + "|-", item[0])


# iterate_through_object(h5file)

h5file.close()
