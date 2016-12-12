# If positional data comes in separate files, they should be combined into one.
# Suggested method of doing this:
# > cat ./LogData* | sort | uniq | sed '1h;1d;$!H;$!d;G' > posdata.csv
# If you're a real beast you can pipe this to xargs while invoking this script as well, but I haven't been that brave yet.

import h5py
import numpy as np
import datetime
import pandas as pd
import katpoint

# Set up option parser
from optparse import OptionParser
option_parser = OptionParser(usage="python %prog [options] h5filename csvfilename pmodlfilename")

# Not using any options at this stage, just the arguments. May want more flexibility in future.
(options, args) = option_parser.parse_args()

if not (len(args) == 3 or len(args) == 2):
    option_parser.error("Wrong number of arguments - two or three filenames expected, %d arguments received."%(len(args)))

try:
    print "Opening %s for augmenting..."%(args[0])
    h5file = h5py.File(name=args[0], mode='r+')
    print "File successfully opened."
except IOError:
    print "Error opening h5 file! Check spelling and path."
    exit()

try:
    print "Opening %s for antenna position data..."%(args[1])
    csv_file = pd.read_csv(args[1], skipinitialspace=True)
    print "File successfully opened."
except IOError:
    print "Error opening csv file! Check spelling and path."
    exit()
except pd.parser.CParserError:
    # TODO: point to the line number. Figure out how to get exception's text.
    print "Line size problem in csv file. Must fix manually. Easiest to do this using a spreadsheet package."
    exit()

try:
    print "Opening %s for FS pointing model..."%(args[2])
    pmodl_file = open(args[2])
except (IOError, IndexError):
    print "Error opening pointing model file! Check spelling and path. Using default (zero) pointing model."
    pmodl_file = None


# Credit to http://stackoverflow.com/a/12737895 for this function:
def decdeg2dms(dd):
    negative = dd < 0
    dd = abs(dd)
    minutes,seconds = divmod(dd*3600,60)
    degrees,minutes = divmod(minutes,60)
    if negative:
        if degrees > 0:
            degrees = -degrees
        elif minutes > 0:
            minutes = -minutes
        else:
            seconds = -seconds
    return (int(degrees),int(minutes),float(seconds))

def fs_to_kp_pointing_model(pmodl_file):
    """Parses a Field System pointing model file (mdlpo.ctl)
    Returns:
        katpoint pointing model (object? string?)
    """
    if pmodl_file == None:
        pmodl_string = "0 " * 22
        pmodl_string = pmodl_string[:-1]  # Remove the resulting space on the end.
        return pmodl_string

    if not isinstance(pmodl_file, file):
        raise TypeError("%s not a text file."%(repr(pmodl_file)))
    lines = []
    for i in pmodl_file:
        lines.append(i)
    if len(lines) != 19:
        raise TypeError("%s not correct length for pointing model file. File is %d lines long."%(repr(pmodl_file), len(lines)))
    # Line 5 gives the enabled parameters:
    params_implemented = lines[5].split()

    if len(params_implemented) != 31:
        raise TypeError("%s not correct format for pointing model file." % (repr(pmodl_file)))
    # The first number on the line is the phi value
    phi = float(params_implemented[0])

    # If any of the higher ones are used, throw a warning: (TODO: figure out how to do this properly in Python. There must be a prettier way.)
    if params_implemented[23] == '1' or \
       params_implemented[24] == '1' or \
       params_implemented[25] == '1' or \
       params_implemented[26] == '1' or \
       params_implemented[27] == '1' or \
       params_implemented[28] == '1' or \
       params_implemented[29] == '1' or \
       params_implemented[30] == '1':
        print "Warning: params 23 - 30 are not used in Katpoint, but are used in the pointing model file."

    # Lines 7, 9, 11, 13, 15 and 17 each have 5 parameters on them.
    params = [ 0 ] # Ppad place number 0 so that the numbers correspond.
    params.extend(lines[7].split())
    params.extend(lines[9].split())
    params.extend(lines[11].split())
    params.extend(lines[13].split())
    params.extend(lines[15].split())
    params.extend(lines[17].split())

    pmodl_string = ""

    for i in range(1,23):
        if params_implemented[i] == '1' and float(params[i]) != 0:
            pmodl_string += "%02d:%02d:%06.3f "%(decdeg2dms(float(params[i]))) # I had thought that Katpoint needs dd:mm:ss.xx format, but apparently it doesn't.
            # pmodl_string += "%s "%(params[i])

        else:
            pmodl_string += "0 "

    pmodl_string = pmodl_string[:-1] # Remove the resulting space on the end.
    return pmodl_string


##### Miscellaneous info about the file printed for the user's convenience. #####
timestamps = h5file["Data/Timestamps"]
rf_begin_time = timestamps[0]
rf_end_time = timestamps[-1]
duration = rf_end_time - rf_begin_time
duration_str = "%dh %dm %.2fs"%( int(duration/3600), int(duration - int(duration/3600)*3600)/60, duration - int(duration/3600)*3600 - (int(duration - int(duration/3600)*3600)/60)*60)
print "Recording start:\t\t%s UTC\nRecording end:\t\t\t%s UTC\nDuration:\t\t\t%s\n"%(datetime.datetime.fromtimestamp(rf_begin_time).isoformat(), datetime.datetime.fromtimestamp(rf_end_time).isoformat(), duration_str)

vis_shape = h5file["Data/VisData"].shape

print "Accumulation length:\t\t%.2f ms"%((timestamps[1] - timestamps[0])*1000.0)
print "Number of accums:\t\t%d"%(vis_shape[0] - 1)
print "Number of frequency channels:\t%d"%(vis_shape[1])


sensor_group = h5file["MetaData/Sensors"]
config_group = h5file["MetaData/Configuration"]

##### Weather / Environment information #####
# This section ought only to be here temporarily. At time of writing (Oct 2016), the
# site weather station at Kuntunse isn't available, so we will do some mock weather
# data to satisfy katpoint's requirements.
# This section can later be adapted to read real data once the data becomes available.

while True: # because Python doesn't have do..while loops... :-/
    dummy = raw_input("Add default (dummy) enviroment sensor data? (y/n) ")
    try:
        if (dummy.lower()[0] == 'y') or (dummy.lower() == 'n'):
            break
        else:
            print "Invalid response."
    except IndexError:
        print "Invalid response."
        pass

# TODO: Think about putting some sort of attribute somewhere to test whether or not the file has already been augmented.
#       Then the other try / except blocks shouldn't be necessary.

try:
    enviro_group = sensor_group.create_group("Enviro")
except ValueError:
    print "Warning! Enviro group already exists. File may already have been previously augmented. Carry on regardless."
    enviro_group = sensor_group["Enviro"]

enviro_timestamps = np.arange(rf_begin_time, rf_end_time, 10, dtype=np.float64) # The enviro sensor data needn't be high resolution, 10 seconds for dummy data should be okay.

temperature_array   = []
pressure_array      = []
humidity_array      = []
windspeed_array     = []
winddirection_array = []


if dummy.lower()[0] == 'y': # Use the default dummy data
    temperature   =   30.0 # degC
    pressure      = 1010.0 # mbar
    humidity      =   80.0 # percent
    windspeed     =    0.0 # m/s
    winddirection =    0.0 # deg (heading)
    enviro_group.attrs["note"] = "Augmented with dummy default data."
else:
    temperature   = float(raw_input("Please enter the temperature (deg C): "))
    pressure      = float(raw_input("Please enter the pressure (mbar): "))
    humidity      = float(raw_input("Please enter the relative humidity (percent): "))
    windspeed     = float(raw_input("Please enter the wind speed (m/s): "))
    winddirection = float(raw_input("Please enter the wind direction (degrees CW from N): "))
    enviro_group.attrs["note"] = "Augmented with user-provided static environment data."

for time in enviro_timestamps:
        temperature_array.append((time, temperature, "nominal"))
        pressure_array.append((time, pressure, "nominal"))
        humidity_array.append((time, humidity, "nominal"))
        windspeed_array.append((time, windspeed, "nominal"))
        winddirection_array.append((time, winddirection, "nominal"))

temperature_dset   = np.array(temperature_array, dtype=[('timestamp','<f8'),('value', '<f8'),('status', 'S7')])
pressure_dset      = np.array(pressure_array, dtype=[('timestamp','<f8'),('value', '<f8'),('status', 'S7')])
humidity_dset      = np.array(humidity_array, dtype=[('timestamp','<f8'),('value', '<f8'),('status', 'S7')])
windspeed_dset     = np.array(windspeed_array, dtype=[('timestamp','<f8'),('value', '<f8'),('status', 'S7')])
winddirection_dset = np.array(winddirection_array, dtype=[('timestamp','<f8'),('value', '<f8'),('status', 'S7')])

try:
    print "\nPopulating air temperature dataset with %.2f degrees..."%(temperature)
    enviro_group.create_dataset("air.temperature", data=temperature_dset)
    enviro_group["air.temperature"].attrs["description"] = "Air temperature"
    enviro_group["air.temperature"].attrs["name"]        = "air.temperature"
    enviro_group["air.temperature"].attrs["type"]        = "float64"
    enviro_group["air.temperature"].attrs["units"]       = "degC"

    print "Populating air pressure dataset with %.2f mbar..."%(pressure)
    enviro_group.create_dataset("air.pressure", data=pressure_dset)
    enviro_group["air.pressure"].attrs["description"] = "Air pressure"
    enviro_group["air.pressure"].attrs["name"]        = "air.pressure"
    enviro_group["air.pressure"].attrs["type"]        = "float64"
    enviro_group["air.pressure"].attrs["units"]       = "mbar"

    print "Populating relative humidity dataset with %.2f percent..."%(humidity)
    enviro_group.create_dataset("relative.humidity", data=humidity_dset)
    enviro_group["relative.humidity"].attrs["description"] = "Relative humidity"
    enviro_group["relative.humidity"].attrs["name"]        = "relative.humidity"
    enviro_group["relative.humidity"].attrs["type"]        = "float64"
    enviro_group["relative.humidity"].attrs["units"]       = "percent"

    print "Populating wind speed dataset with %.2f m/s..."%(windspeed)
    enviro_group.create_dataset("wind.speed", data=windspeed_dset)
    enviro_group["wind.speed"].attrs["description"] = "Wind speed"
    enviro_group["wind.speed"].attrs["name"]        = "wind.speed"
    enviro_group["wind.speed"].attrs["type"]        = "float64"
    enviro_group["wind.speed"].attrs["units"]       = "m/s"

    print "Populating wind direction dataset with %.2f degrees (bearing)..."%(winddirection)
    enviro_group.create_dataset("wind.direction", data=winddirection_dset)
    enviro_group["wind.direction"].attrs["description"] = "Wind direction"
    enviro_group["wind.direction"].attrs["name"]        = "wind.direction"
    enviro_group["wind.direction"].attrs["type"]        = "float64"
    enviro_group["wind.direction"].attrs["units"]       = "degrees (bearing)"
except RuntimeError:
    print "Environment sensor dataset(s) already exist. Datafile previously augmented?"
else:
    print "Environment data written."


##### Antenna position information #####

print "\nChecking if sterile datasets were created by the RoachAcquisitionServer."

antenna_pos_dataset_list = ["target",
                            "activity",
                            "pos.actual-dec",
                            "pos.actual-ra",
                            "pos.actual-scan-azim",
                            "pos.actual-scan-elev",
                            "pos.request-scan-azim",
                            "pos.request-scan-elev",
                            "pos.actual-pointm-azim",
                            "pos.actual-pointm-elev",
                            "pos.request-pointm-azim",
                            "pos.request-pointm-elev",
                            "pos.source-offset-azim",
                            "pos.source-offset-elev",
                            "motor-torque.az-master",
                            "motor-torque.az-slave",
                            "motor-torque.el-master",
                            "motor-torque.el-slave"]

# TODO: At the moment the assumption is that the RoachAcquisitionServer is making sterile datasets here and they need to be removed.
# This may at some stage not be true, and it would probably help to include some protection against re-augmenting a data file by accident.
for dset_name in antenna_pos_dataset_list:
    try:
        del sensor_group["Antennas/ant1/%s"%(dset_name)]
    except KeyError:
        print "No sterile %s dataset found."%(dset_name)
    else:
        print "Sterile %s dataset removed."%(dset_name)

print "\nOpening %s for position sensor addition..."%(args[1])
# Check to see that the files line up in at least some way.

pos_begin_time = float(csv_file["Timestamp"][0]) / 1000.0
pos_end_time   = float(csv_file["Timestamp"][len(csv_file["Timestamp"]) - 1]) / 1000.0
csv_duration = pos_end_time - pos_begin_time
csv_duration_str = "%dh %dm %.2fs"%( int(csv_duration/3600), int(csv_duration - int(csv_duration/3600)*3600)/60, csv_duration - int(csv_duration/3600)*3600 - (int(csv_duration - int(csv_duration/3600)*3600)/60)*60)
print "Pos data start:\t\t%s UTC\nPos data end:\t\t%s UTC\nDuration:\t\t%s\n"%(datetime.datetime.fromtimestamp(pos_begin_time).isoformat(), datetime.datetime.fromtimestamp(pos_end_time).isoformat(), csv_duration_str)

complete_overlap = True # This could probably be named better.

rf_begins_after_pos_ends   = rf_begin_time > pos_end_time
rf_ends_before_pos_begins  = rf_end_time < pos_begin_time

if rf_begins_after_pos_ends or rf_ends_before_pos_begins:
    print "RF and position data do not overlap at all. Nothing more to do here..."
    h5file.close()
    exit()

continue_process = True

adjust_begin     = True
adjust_end       = True

rf_begins_after_pos_begins = rf_begin_time > pos_begin_time
rf_ends_before_pos_ends    = rf_end_time   < pos_end_time

if not rf_begins_after_pos_begins:
    adjust_begin     = False
    continue_process = False
    print "Pos data starts after RF data."
elif not rf_ends_before_pos_ends:
    adjust_end       = False
    continue_process = False
    print "Pos data finishes before RF data."

if not continue_process:
    while True:
        user_input = raw_input("Continue the process? (y/n) ")
        if user_input.lower() in ["yes", "y"]:
            continue_process = True
            break
        elif user_input.lower() in ["no", "n"]:
            continue_process = False
            print "Exiting..."
            h5file.close()
            exit()
            break
        else:
            print "Invalid input."

try:
    pos_lower_index = 0
    if adjust_begin:
        while (float(csv_file["Timestamp"][pos_lower_index]) / 1000.0) < rf_begin_time:
            pos_lower_index += 1
            # While loop will break when it gets lower or equal to
        pos_lower_index -= 1 # ... and one step back to ensure complete coverage

    print "\nLower bound: %d."%(pos_lower_index)
    print "CSV lower index: %.2f\tH5file lower index: %.2f"%(csv_file["Timestamp"][pos_lower_index] / 1000.0, rf_begin_time)
    print "Position data commences %.2f seconds %s start of RF data."%(np.abs(csv_file["Timestamp"][pos_lower_index] / 1000.0 - rf_begin_time), "before" if adjust_begin else "after")
except ValueError:
    print "Error. There are funny lines in the CSV file. They might be somewhere around line %d."%(pos_lower_index)
    h5file.close()
    exit()

try:
    pos_upper_index = len(csv_file["Timestamp"]) - 1
    if adjust_end:
        while (float(csv_file["Timestamp"][pos_upper_index]) / 1000.0) > rf_end_time:
            pos_upper_index -= 1
            # While loop will break when it gets lower or equal to
        pos_upper_index += 1 # Set it back to just after the RF data ends.
    print "\nUpper bound: %d."%(pos_upper_index)
    print "CSV upper index: %.2f\tH5file upper index: %.2f"%(csv_file["Timestamp"][pos_upper_index] / 1000.0, rf_end_time)
    print "Position data extens to %.2f seconds %s RF data."%(np.abs(csv_file["Timestamp"][pos_upper_index] / 1000.0 - rf_end_time), "after" if adjust_end else "before")
except ValueError:
    print "There is a fault in the CSV file. It might be somewhere around line %d."%(pos_upper_index)
    h5file.close()
    exit()

# What remains to do is to have the relevant data in numpy arrays ready for the splicing into the HDF5 file.

timestamp_array = np.array(csv_file["Timestamp"][pos_lower_index:pos_upper_index], dtype=[("timestamp", "<f8")])

# Unfortunately no elegant way to do this really... as far as I can tell.
target_dset           = []
activity_dset         = []

azim_req_pointm_pos_dset = []
azim_des_pointm_pos_dset = []
azim_act_pointm_pos_dset = []
elev_req_pointm_pos_dset = []
elev_des_pointm_pos_dset = []
elev_act_pointm_pos_dset = []

azim_req_scan_pos_dset = []
azim_des_scan_pos_dset = []
azim_act_scan_pos_dset = []
elev_req_scan_pos_dset = []
elev_des_scan_pos_dset = []
elev_act_scan_pos_dset = []

antenna_sensor_group = sensor_group["Antennas/ant1"]

# TODO: This should ideally come from the file, not be hardcoded here.
# Note: The 0 0 0 just before the %s is the "delay model" which katpoint expects. We don't use it.
antenna_str = "Kuntunse, 5:45:2.48, -0:18:17.92, 116, 32.0, 0 0 0, %s"%(fs_to_kp_pointing_model(pmodl_file))
config_group["Antennas/ant1"].attrs["description"] = antenna_str
antenna = katpoint.Antenna(antenna_str)
print antenna.pointing_model
activity = "slew"

target = raw_input("Enter target in katpoint string format:\n(name, radec, 00:00:00.00, 00:00:00.00)\n")
target_dset.append((float(csv_file["Timestamp"][pos_lower_index]) / 1000.0, target, "nominal"))
print "Writing target data..."
target_dset = np.array(target_dset, dtype=[("timestamp", "<f8"), ("value", "S127"), ("status", "S7")])
antenna_sensor_group.create_dataset("target", data=target_dset)
antenna_sensor_group["target"].attrs["description"] = "Current target"
antenna_sensor_group["target"].attrs["name"] = "target"
antenna_sensor_group["target"].attrs["type"] = "string"
antenna_sensor_group["target"].attrs["units"] = ""



print "Reading position data from csv file into memory..."
activity_dset.append((csv_file["Timestamp"][pos_lower_index] / 1000.0, "slew", "nominal"))

for i in range(0, len(timestamp_array), 20): #Down-sample by a factor of 20
    azim_req_pointm_pos_dset.append((float(csv_file["Timestamp"][pos_lower_index + i]) / 1000.0, csv_file["Azim req position"][pos_lower_index + i], "nominal"))
    azim_des_pointm_pos_dset.append((float(csv_file["Timestamp"][pos_lower_index + i]) / 1000.0, csv_file["Azim desired position"][pos_lower_index + i], "nominal"))
    azim_act_pointm_pos_dset.append((float(csv_file["Timestamp"][pos_lower_index + i]) / 1000.0, csv_file["Azim actual position"][pos_lower_index + i], "nominal"))
    elev_req_pointm_pos_dset.append((float(csv_file["Timestamp"][pos_lower_index + i]) / 1000.0, csv_file["Elev req position"][pos_lower_index + i], "nominal"))
    elev_des_pointm_pos_dset.append((float(csv_file["Timestamp"][pos_lower_index + i]) / 1000.0, csv_file["Elev desired position"][pos_lower_index + i], "nominal"))
    elev_act_pointm_pos_dset.append((float(csv_file["Timestamp"][pos_lower_index + i]) / 1000.0, csv_file["Elev actual position"][pos_lower_index + i], "nominal"))

    azim_req_scan_pos_dset.append((float(csv_file["Timestamp"][pos_lower_index + i]) / 1000.0,
                                   np.degrees(antenna.pointing_model.reverse(np.radians(csv_file["Azim req position"][pos_lower_index + i]),
                                                                             np.radians(csv_file["Elev req position"][pos_lower_index + i]))[0]),
                                   "nominal"))
    elev_req_scan_pos_dset.append((float(csv_file["Timestamp"][pos_lower_index + i]) / 1000.0,
                                   np.degrees(antenna.pointing_model.reverse(np.radians(csv_file["Azim req position"][pos_lower_index + i]),
                                                                             np.radians(csv_file["Elev req position"][pos_lower_index + i]))[1]),
                                   "nominal"))
    azim_des_scan_pos_dset.append((float(csv_file["Timestamp"][pos_lower_index + i]) / 1000.0,
                                   np.degrees(antenna.pointing_model.reverse(np.radians(csv_file["Azim desired position"][pos_lower_index + i]),
                                                                             np.radians(csv_file["Elev desired position"][pos_lower_index + i]))[0]),
                                   "nominal"))
    elev_des_scan_pos_dset.append((float(csv_file["Timestamp"][pos_lower_index + i]) / 1000.0,
                                   np.degrees(antenna.pointing_model.reverse(np.radians(csv_file["Azim desired position"][pos_lower_index + i]),
                                                                             np.radians(csv_file["Elev desired position"][pos_lower_index + i]))[1]),
                                   "nominal"))
    azim_act_scan_pos_dset.append((float(csv_file["Timestamp"][pos_lower_index + i]) / 1000.0,
                                   np.degrees(antenna.pointing_model.reverse(np.radians(csv_file["Azim actual position"][pos_lower_index + i]),
                                                                             np.radians(csv_file["Elev actual position"][pos_lower_index + i]))[0]),
                                   "nominal"))
    elev_act_scan_pos_dset.append((float(csv_file["Timestamp"][pos_lower_index + i]) / 1000.0,
                                   np.degrees(antenna.pointing_model.reverse(np.radians(csv_file["Azim actual position"][pos_lower_index + i]),
                                                                             np.radians(csv_file["Elev actual position"][pos_lower_index + i]))[1]),
                                   "nominal"))

    req_target = katpoint.construct_azel_target(csv_file["Azim req position"][pos_lower_index + i], csv_file["Elev req position"][pos_lower_index + i])
    req_target.antenna = antenna
    actual_target = katpoint.construct_azel_target(csv_file["Azim actual position"][pos_lower_index + i], csv_file["Elev actual position"][pos_lower_index + i])
    actual_target.antenna = antenna
    if activity == "slew":
        if actual_target.separation(req_target) < 0.005: # a twentieth of a HPBW
            activity_dset.append((csv_file["Timestamp"][pos_lower_index + i] / 1000.0, "scan", "nominal"))
            activity = "scan"
    else:
        #if activity == "scan":
        if actual_target.separation(req_target) > 0.05: # Half of a HPBW
            activity_dset.append((csv_file["Timestamp"][pos_lower_index + i] / 1000.0, "slew", "nominal"))
            activity = "slew"

print "Writing activity data..."
activity_dset = np.array(activity_dset, dtype=[("timestamp", "<f8"), ("value", "S13"), ("status", "S7")])
antenna_sensor_group.create_dataset("activity", data=activity_dset)
antenna_sensor_group["activity"].attrs["description"] = "Synthesised antenna behaviour label"
antenna_sensor_group["activity"].attrs["name"] = "activity"
antenna_sensor_group["activity"].attrs["type"] = "discrete"
antenna_sensor_group["activity"].attrs["units"] = ""

print "Writing requested azimuth..."
azim_req_pointm_pos_dset = np.array(azim_req_pointm_pos_dset, dtype=[('timestamp', '<f8'), ('value', '<f8'), ('status', 'S7')])
antenna_sensor_group.create_dataset("pos.request-pointm-azim", data=azim_req_pointm_pos_dset)
antenna_sensor_group["pos.request-pointm-azim"].attrs["description"] = "Requested (by user or Field System) azimuth position."
antenna_sensor_group["pos.request-pointm-azim"].attrs["name"] = "pos.request-pointm-azim"
antenna_sensor_group["pos.request-pointm-azim"].attrs["type"] = "float64"
antenna_sensor_group["pos.request-pointm-azim"].attrs["units"] = "degrees CW from N"

azim_req_scan_pos_dset = np.array(azim_req_scan_pos_dset, dtype=[('timestamp', '<f8'), ('value', '<f8'), ('status', 'S7')])
antenna_sensor_group.create_dataset("pos.request-scan-azim", data=azim_req_scan_pos_dset)
antenna_sensor_group["pos.request-scan-azim"].attrs["description"] = "Requested (by user or Field System) azimuth position."
antenna_sensor_group["pos.request-scan-azim"].attrs["name"] = "pos.request-scan-azim"
antenna_sensor_group["pos.request-scan-azim"].attrs["type"] = "float64"
antenna_sensor_group["pos.request-scan-azim"].attrs["units"] = "degrees CW from N"

print "Writing desired azimuth..."
azim_des_pointm_pos_dset = np.array(azim_des_pointm_pos_dset, dtype=[('timestamp', '<f8'), ('value', '<f8'), ('status', 'S7')])
antenna_sensor_group.create_dataset("pos.desired-pointm-azim", data=azim_des_pointm_pos_dset)
antenna_sensor_group["pos.desired-pointm-azim"].attrs["description"] = "Intermediate azimuth position setpoint used by the ASCS."
antenna_sensor_group["pos.desired-pointm-azim"].attrs["name"] = "pos.desired-pointm-azim"
antenna_sensor_group["pos.desired-pointm-azim"].attrs["type"] = "float64"
antenna_sensor_group["pos.desired-pointm-azim"].attrs["units"] = "degrees CW from N"

azim_des_scan_pos_dset = np.array(azim_des_scan_pos_dset, dtype=[('timestamp', '<f8'), ('value', '<f8'), ('status', 'S7')])
antenna_sensor_group.create_dataset("pos.desired-scan-azim", data=azim_des_scan_pos_dset)
antenna_sensor_group["pos.desired-scan-azim"].attrs["description"] = "Intermediate azimuth position setpoint used by the ASCS."
antenna_sensor_group["pos.desired-scan-azim"].attrs["name"] = "pos.desired-scan-azim"
antenna_sensor_group["pos.desired-scan-azim"].attrs["type"] = "float64"
antenna_sensor_group["pos.desired-scan-azim"].attrs["units"] = "degrees CW from N"

print "Writing actual azimuth..."
azim_act_pointm_pos_dset = np.array(azim_act_pointm_pos_dset, dtype=[('timestamp', '<f8'), ('value', '<f8'), ('status', 'S7')])
antenna_sensor_group.create_dataset("pos.actual-pointm-azim", data=azim_act_pointm_pos_dset)
antenna_sensor_group["pos.actual-pointm-azim"].attrs["description"] = "Azimuth data returned by the encoder."
antenna_sensor_group["pos.actual-pointm-azim"].attrs["name"] = "pos.actual-pointm-azim"
antenna_sensor_group["pos.actual-pointm-azim"].attrs["type"] = "float64"
antenna_sensor_group["pos.actual-pointm-azim"].attrs["units"] = "degrees CW from N"

azim_act_scan_pos_dset = np.array(azim_act_scan_pos_dset, dtype=[('timestamp', '<f8'), ('value', '<f8'), ('status', 'S7')])
antenna_sensor_group.create_dataset("pos.actual-scan-azim", data=azim_act_scan_pos_dset)
antenna_sensor_group["pos.actual-scan-azim"].attrs["description"] = "Azimuth data returned by the encoder."
antenna_sensor_group["pos.actual-scan-azim"].attrs["name"] = "pos.actual-scan-azim"
antenna_sensor_group["pos.actual-scan-azim"].attrs["type"] = "float64"
antenna_sensor_group["pos.actual-scan-azim"].attrs["units"] = "degrees CW from N"

print "Writing requested elevation..."
elev_req_pointm_pos_dset = np.array(elev_req_pointm_pos_dset, dtype=[('timestamp', '<f8'), ('value', '<f8'), ('status', 'S7')])
antenna_sensor_group.create_dataset("pos.request-pointm-elev", data=elev_req_pointm_pos_dset)
antenna_sensor_group["pos.request-pointm-elev"].attrs["description"] = "Requested (by user or Field System) elevation position."
antenna_sensor_group["pos.request-pointm-elev"].attrs["name"] = "pos.request-pointm-elev"
antenna_sensor_group["pos.request-pointm-elev"].attrs["type"] = "float64"
antenna_sensor_group["pos.request-pointm-elev"].attrs["units"] = "degrees CW from N"

elev_req_scan_pos_dset = np.array(elev_req_scan_pos_dset, dtype=[('timestamp', '<f8'), ('value', '<f8'), ('status', 'S7')])
antenna_sensor_group.create_dataset("pos.request-scan-elev", data=elev_req_scan_pos_dset)
antenna_sensor_group["pos.request-scan-elev"].attrs["description"] = "Requested (by user or Field System) elevation position."
antenna_sensor_group["pos.request-scan-elev"].attrs["name"] = "pos.request-scan-elev"
antenna_sensor_group["pos.request-scan-elev"].attrs["type"] = "float64"
antenna_sensor_group["pos.request-scan-elev"].attrs["units"] = "degrees CW from N"

print "Writing desired elevation..."
elev_des_pointm_pos_dset = np.array(elev_des_pointm_pos_dset, dtype=[('timestamp', '<f8'), ('value', '<f8'), ('status', 'S7')])
antenna_sensor_group.create_dataset("pos.desired-pointm-elev", data=elev_des_pointm_pos_dset)
antenna_sensor_group["pos.desired-pointm-elev"].attrs["description"] = "Intermediate elevation position setpoint used by the ASCS."
antenna_sensor_group["pos.desired-pointm-elev"].attrs["name"] = "pos.desired-pointm-elev"
antenna_sensor_group["pos.desired-pointm-elev"].attrs["type"] = "float64"
antenna_sensor_group["pos.desired-pointm-elev"].attrs["units"] = "degrees CW from N"

elev_des_scan_pos_dset = np.array(elev_des_scan_pos_dset, dtype=[('timestamp', '<f8'), ('value', '<f8'), ('status', 'S7')])
antenna_sensor_group.create_dataset("pos.desired-scan-elev", data=elev_des_scan_pos_dset)
antenna_sensor_group["pos.desired-scan-elev"].attrs["description"] = "Intermediate elevation position setpoint used by the ASCS."
antenna_sensor_group["pos.desired-scan-elev"].attrs["name"] = "pos.desired-scan-elev"
antenna_sensor_group["pos.desired-scan-elev"].attrs["type"] = "float64"
antenna_sensor_group["pos.desired-scan-elev"].attrs["units"] = "degrees CW from N"

# TODO: This needs to change back to 'pointm' at some point. I've fudged it into 'scan' so that scape will read it.
print "Writing actual elevation..."
elev_act_pointm_pos_dset = np.array(elev_act_pointm_pos_dset, dtype=[('timestamp', '<f8'), ('value', '<f8'), ('status', 'S7')])
antenna_sensor_group.create_dataset("pos.actual-pointm-elev", data=elev_act_pointm_pos_dset)
antenna_sensor_group["pos.actual-pointm-elev"].attrs["description"] = "Elevation data returned by the encoder."
antenna_sensor_group["pos.actual-pointm-elev"].attrs["name"] = "pos.actual-pointm-elev"
antenna_sensor_group["pos.actual-pointm-elev"].attrs["type"] = "float64"
antenna_sensor_group["pos.actual-pointm-elev"].attrs["units"] = "degrees CW from N"

elev_act_scan_pos_dset = np.array(elev_act_scan_pos_dset, dtype=[('timestamp', '<f8'), ('value', '<f8'), ('status', 'S7')])
antenna_sensor_group.create_dataset("pos.actual-scan-elev", data=elev_act_scan_pos_dset)
antenna_sensor_group["pos.actual-scan-elev"].attrs["description"] = "Elevation data returned by the encoder."
antenna_sensor_group["pos.actual-scan-elev"].attrs["name"] = "pos.actual-scan-elev"
antenna_sensor_group["pos.actual-scan-elev"].attrs["type"] = "float64"
antenna_sensor_group["pos.actual-scan-elev"].attrs["units"] = "degrees CW from N"

### Misc other things. ###

print "Adding dummy 'label' data to the datafile."
del h5file["Markup/labels"]
label_dset = [(csv_file["Timestamp"][pos_lower_index], "avn_dummy", "nominal")]
label_dset = np.array(label_dset, dtype=[("timestamp","<f8"), ("label","S13"), ("status","S7")])
h5file["Markup"].create_dataset("labels", data=label_dset)

print "\nAugmentation complete. Closing HDF5 file."
h5file.close()
