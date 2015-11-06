# import numpy as np
import h5py
import sys

h5file = h5py.File(sys.argv[1], "r+")

h5file.create_group("Metadata")
h5file["Metadata"].create_group("Configuration")
h5file["Metadata/Configuration"].create_group("Observation")
h5file["Metadata/Configuration/Observation"].attrs[""] = ""  # Duplicate this line a lot of times.
# # These are the attributes of "Configuration/Observation"
# ants = ant5,ant4,ant3,ant2
# script_arguments = --stow-when-done -m 7200 --proposal-id=COMM_RFI_SP --program-block-id=00ob3000000 --sb-id-code=20151024-0004 --description=RFI SCAN --observer=Sean
# description = Basic RFI Scan: RFI SCAN
# endtime = 1445690968.64
# experiment_id = 20151024-0004
# script_name = /home/kat/scripts/observation/rfi_scan.py
# nd_params = Diode=coupler, On=10 s, Off=10 s, Period=180 s
# observer = Sean
# rf_params = Centre freq=1328 MHz, Dump rate=1 Hz
# starttime = 1445685605.52
# status = interrupte        script_ants = ant5,ant4,ant3,ant2
# arguments = --stow-when-done -m 7200 --proposal-id=COMM_RFI_SP --program-block-id=00ob3000000 --sb-id-code=20151024-0004 --description=RFI SCAN --observer=Sean
# description = Basic RFI Scan: RFI SCAN
# endtime = 1445690968.64
# experiment_id = 20151024-0004
# name = /home/kat/scripts/observation/rfi_scan.py
# nd_params = Diode=coupler, On=10 s, Off=10 s, Period=180 s
# observer = Sean
# rf_params = Centre freq=1328 MHz, Dump rate=1 Hz
# starttime = 1445685605.52
# status = interrupted



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
