# import numpy as np
import h5py
import sys
import numpy as np

h5file = h5py.File(sys.argv[1], "r+")

h5file.attrs["version"] = "4.0"

h5file.create_group("Markup")

h5file.create_group("MetaData")
h5file["MetaData"].create_group("Configuration")
h5file["MetaData/Configuration"].create_group("Observation")
obsAttrs = {"ants": "ant1",
            "script_arguments": "augmented with dummy data",
            "description":      "Noise generator data",
            "endtime":          "unix time when the observation finished.",
            "experiment_id":    "YYYYMMDD-0001",
            "script_name":      "/home/kat/scripts/observation/example.py",
            "nd_params":        "Diode=coupler, On=10 s, Off=10 s, Period=180 s",
            "observer":         "Charles Copley",
            "rf_params":        "Centre freq=200 MHz, Dump rate=125 Hz",
            "starttime":        "Unix time when observation started.",
            "status":           "something"
            }
for obsAttr in obsAttrs.iteritems():
    h5file["MetaData/Configuration/Observation"].attrs[obsAttr[0]] = obsAttr[1]

h5file["MetaData/Configuration"].create_group("Correlator")
# TODO: Find out about this. What should they be named to work properly?
corrAttrs = {
    "bls_ordering": [["ant1l","ant1l"],["ant1r","ant1r"],["ant1l","ant1r"],["ant1l","ant1r"]],
    "input_map": "placeholder"
}
for corrAttr in corrAttrs.iteritems():
    h5file["MetaData/Configuration/Correlator"].attrs[corrAttr[0]] = corrAttr[1]
h5file["MetaData/Configuration/Correlator"].create_dataset("int_time", shape=(1, 1), dtype=np.float64)
h5file["MetaData/Configuration/Correlator/int_time"][0] = 0.008

h5file["MetaData/Configuration"].create_group("Antennas")
h5file["MetaData/Configuration/Antennas"].create_group("ant1")
h5file["MetaData/Configuration/Antennas/ant1"].attrs["name"]           = "ant1"
h5file["MetaData/Configuration/Antennas/ant1"].attrs["latitude"]       = "5:45:1.8462"
h5file["MetaData/Configuration/Antennas/ant1"].attrs["longitude"]      = "0:18:18.741"
h5file["MetaData/Configuration/Antennas/ant1"].attrs["altitude"]       = "60"
h5file["MetaData/Configuration/Antennas/ant1"].attrs["diameter"]       = "32"
h5file["MetaData/Configuration/Antennas/ant1"].attrs["delay_model"]    = "0"
h5file["MetaData/Configuration/Antennas/ant1"].attrs["pointing_model"] = "0"
h5file["MetaData/Configuration/Antennas/ant1"].attrs["beamwidth"]      = "0.1"

h5file["MetaData"].create_group("Sensors")

# Centre frequency stuff
start_time = h5file["Data/Timestamps"][0]
end_time   = h5file["Data/Timestamps"][-1]
# print "Times: %f, %f, difference: %f"%(start_time, end_time, end_time - start_time)
n_cent_freq_samples = int((end_time - 2 - start_time)/10)  # Minus two somewhat arbitrarily, so that we don't get something coinciding with the last sample.
cent_freq_samples = np.arange(n_cent_freq_samples, dtype=np.float64)
cent_freq_sample_spacing = (end_time - start_time)/n_cent_freq_samples
cent_freq_samples *= cent_freq_sample_spacing
cent_freq_samples += start_time
cent_freq_samples += np.random.randn(len(cent_freq_samples))  # Because a bit of randomness is a bit more fun. To see what katdal does with it.
# This line is important, the dtype is important in building the h5py dataset properly.
cf_dataset = np.array([(timestamp, float(200e6), "nominal") for timestamp in cent_freq_samples], dtype=[('timestamp', '<f8'), ('value', '<f8'), ('status', 'S7')])
h5file["MetaData/Sensors"].create_group("RFE")
h5file["MetaData/Sensors/RFE"].create_dataset("center-frequency-hz", data=cf_dataset)

h5file["MetaData/Configuration/Correlator"].create_dataset("n_chans", shape=(1, 1), dtype=np.int32)
h5file["MetaData/Configuration/Correlator/n_chans"][0] = h5file["Data/SingleDishData"].shape[1]

h5file["MetaData/Configuration/Correlator"].create_dataset("bandwidth", shape=(1, 1), dtype=np.float64)
if h5file["Data/SingleDishData"].shape[1] == 1024:
    h5file["MetaData/Configuration/Correlator/bandwidth"][0] = 400.0e6
elif h5file["Data/SingleDishData"].shape[1] == 4096:
    h5file["MetaData/Configuration/Correlator/bandwidth"][0] = 1562500.0
else:
    raise AttributeError("There's something wrong here. The number of channels seems wrong: %d, expected either 1024 or 4096.")



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
