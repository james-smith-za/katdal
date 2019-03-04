#!/usr/bin/env/python

import h5py
import numpy as np
import pandas as pd
import datetime


# Set up option parser
from optparse import OptionParser
option_parser = OptionParser(usage="python %prog h5filename logfilename")

# Not using any options at this stage, just the arguments. May want more flexibility in future.
(options, args) = option_parser.parse_args()

if len(args) != 2:
    option_parser.error("Wrong number of arguments - two filenames expected, %d arguments received." %
                        (len(args)))


def write_dataset(dset_name, location, data, attributes=()):
    """Write a dataset to the location."""
    response = ""
    try:
        dset = location[dset_name]
    except KeyError:
        response += "No sterile %s dataset found. Adding augmented dataset.\n" % dset_name
    else:
        if dset.size != 0:
            response += "Dataset %s already exists, nonzero size, not altered. Was this file previously augmented?\n" %\
                        dset_name
            return response
        del location[dset_name]  # This actually removes the dataset.
        response += "Sterile %s dataset removed. Adding augmented dataset.\n" % dset_name

    dset = location.create_dataset(dset_name, data=data)
    for i in attributes:
        dset.attrs[i] = attributes[i]

    response += "%s dataset successfully added to file." % dset_name
    return response


print "Opening files for augmenting..."
with h5py.File(name=args[0], mode='r+') as h5file:

    # Pandas doesn't support context managers.
    try:
        print "Opening %s for log data..." % (args[1])
        csv_file = pd.read_csv(args[1], skipinitialspace=True)
        print "File successfully opened."
    except IOError:
        print "Error opening csv file! Check spelling and path."
        exit()
    except pd.parser.CParserError:
        print "Line size problem in csv file. Must fix manually. Easiest to do this using a spreadsheet package."
        exit()

    print "Files successfully opened."

    # Miscellaneous info about the file printed for the user's convenience:
    timestamps = h5file["Data/Timestamps"]

    rf_begin_time = timestamps[0]
    rf_end_time = timestamps[-1]
    duration = rf_end_time - rf_begin_time
    duration_str = "%dh %dm %.2fs" % (int(duration/3600),
                                      int(duration - int(duration/3600)*3600)/60,
                                      duration - int(duration/3600)*3600 -
                                      (int(duration - int(duration/3600)*3600)/60)*60)
    print "Recording start:\t\t%s UTC\nRecording end:\t\t\t%s UTC\nDuration:\t\t\t%s\n" %\
          (datetime.datetime.fromtimestamp(rf_begin_time).isoformat(),
           datetime.datetime.fromtimestamp(rf_end_time).isoformat(),
           duration_str)

    vis_shape = h5file["Data/VisData"].shape

    try:
        print "Accumulation length:\t\t%.2f ms" % ((timestamps[1] - timestamps[0])*1000.0)
    except ValueError:
        print "Only one accumulation, uncertain of length."
    print "Number of accums:\t\t%d" % (vis_shape[0] - 1)
    print "Number of frequency channels:\t%d" % (vis_shape[1])

    try:
        history_group = h5file.create_group("History")
    except ValueError:
        print "Warning! History group already exists. Log may already have been written to this file."
        history_group = h5file["History"]

    # Check to see that the files line up in at least some way.
    csv_begin_time = float(csv_file["Timestamp"][0])
    # Milliseconds in the file, divide to get proper unix time.
    csv_end_time = float(csv_file["Timestamp"][len(csv_file["Timestamp"]) - 1])
    csv_duration = csv_end_time - csv_begin_time
    csv_duration_str = "%dh %dm %.2fs" % (int(csv_duration/3600),
                                          int(csv_duration - int(csv_duration/3600)*3600)/60,
                                          csv_duration - int(csv_duration/3600)*3600 -
                                          (int(csv_duration - int(csv_duration/3600)*3600)/60)*60)
    print "CSV data start:\t\t%s UTC\nCSV data end:\t\t%s UTC\nDuration:\t\t%s\n" % \
          (datetime.datetime.fromtimestamp(csv_begin_time).isoformat(),
           datetime.datetime.fromtimestamp(csv_end_time).isoformat(),
           csv_duration_str)

    complete_overlap = True  # This could probably be named better.

    rf_begins_after_csv_ends = rf_begin_time > csv_end_time
    rf_ends_before_csv_begins = rf_end_time < csv_begin_time

    if rf_begins_after_csv_ends or rf_ends_before_csv_begins:
        print "RF and position data do not overlap at all. Nothing more to do here..."
        exit()

    continue_process = True
    adjust_begin = True
    adjust_end = True

    rf_begins_after_pos_begins = rf_begin_time > csv_begin_time
    rf_ends_before_pos_ends = rf_end_time < csv_end_time

    if not rf_begins_after_pos_begins:
        adjust_begin = False
        continue_process = False
        print "CSV data starts after RF data."
    elif not rf_ends_before_pos_ends:
        adjust_end = False
        continue_process = False
        print "CSV data finishes before RF data."

    if not continue_process:
        while True:
            user_input = raw_input("Continue the process? (y/n) ")
            if user_input.lower() in ["yes", "y"]:
                continue_process = True
                break
            elif user_input.lower() in ["no", "n"]:
                continue_process = False
                print "Exiting..."
                exit()
                break
            else:
                print "Invalid input."

    csv_lower_index = 0
    if adjust_begin:
        while (float(csv_file["Timestamp"][csv_lower_index]) ) < rf_begin_time:
            csv_lower_index += 1
            # While loop will break when it gets lower or equal to
        csv_lower_index -= 1  # ... and one step back to ensure complete coverage

    print "\nLower bound: %d." % csv_lower_index
    print "CSV lower index: %.2f\tH5file lower index: %.2f" % \
          (csv_file["Timestamp"][csv_lower_index], rf_begin_time)
    print "Position data commences %.2f seconds %s start of RF data." % \
          (np.abs(csv_file["Timestamp"][csv_lower_index] - rf_begin_time),
           "before" if adjust_begin else "after")

    csv_upper_index = len(csv_file["Timestamp"]) - 1
    if adjust_end:
        while (float(csv_file["Timestamp"][csv_upper_index]) ) > rf_end_time:
            csv_upper_index -= 1
            # While loop will break when it gets lower or equal to
        csv_upper_index += 1  # Set it back to just after the RF data ends.
    print "\nUpper bound: %d." % csv_upper_index
    print "CSV upper index: %.2f\tH5file upper index: %.2f" % \
          (csv_file["Timestamp"][csv_upper_index] / 1000.0, rf_end_time)
    print "Position data extends to %.2f seconds %s RF data." % \
          (np.abs(csv_file["Timestamp"][csv_upper_index] - rf_end_time),
           "after" if adjust_end else "before")

    # What remains to do is to have the relevant data in numpy arrays ready for the splicing into the HDF5 file.
    timestamp_array = np.array(csv_file["Timestamp"][csv_lower_index:csv_upper_index], dtype=[("timestamp", "<f8")])

    log_dset = []

    for i in range(0, len(timestamp_array)):
        log_dset.append((csv_file["Timestamp"][csv_lower_index + i], csv_file["Log"][csv_lower_index + i]))

    log_dset = np.array(log_dset, dtype=[("timestamp", "<f8"), ("value", "S128")])

    print write_dataset("script_log", history_group, data=log_dset)
    h5file.close()
