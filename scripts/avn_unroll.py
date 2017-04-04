#!/usr/bin/env python

import h5py
import numpy as np


def unroll(input_array, bitlength=32):
    """Unroll data which has wrapped around the 32-bit integer limit. Convert to float."""
    deltas = np.diff(input_array)
    output_array = np.array(input_array, dtype=np.float)

    # A simple and efficient method if I don't say so myself...
    for i in range(1,len(deltas)):
        if np.abs(deltas[i]) > float(2**(bitlength-1)):
            output_array[i+1:] -= float(2**bitlength)*np.sign(deltas[i])
            output_array = unroll(output_array)
            break

    return output_array


if __name__ == "__main__":
    # Set up option parser
    from optparse import OptionParser
    option_parser = OptionParser(usage="python %prog [options] h5filename")

    # Not using any options at this stage, just the arguments. May want more flexibility in future.
    (options, args) = option_parser.parse_args()

    if not (len(args) == 1):
        option_parser.error("Wrong number of arguments - only one h5 filename expected, %d arguments received." %
                            (len(args)))

    print "Due to the nature of the beast, the input file cannot be unrolled in-place, therefore a new one will be", \
          "created, the RF data unrolled and the metadata copied from the previous file. This may take a few moments", \
          "depending on the length of the file."
    input_filename = args[0]
    output_filename = input_filename[:-3] + "_unrolled" + input_filename[-3:]

    with h5py.File(input_filename, mode="r") as input_file,  h5py.File(output_filename, mode="w") as output_file:
        filelength = input_file["Data/Timestamps"].size
        print "%d lines to unroll." % filelength

        vis_data = np.array(input_file["Data/VisData"], dtype=np.float)
        for i in range(vis_data.shape[0]):
            vis_data[i, :, 0] = unroll(vis_data[i, :, 0])
            vis_data[i, :, 1] = unroll(vis_data[i, :, 1])
            if i % 20 == 0:
                print "\r%.2f percent complete." % (float(i)/filelength * 100),  # Comma to stop print making a newline

        print "Done unrolling, creating output file..."
        output_file.create_group("Data")
        output_file["Data"].create_dataset("VisData", data=vis_data)

        leftave = np.average(vis_data[:-1, :, 0], axis=1)
        rightave = np.average(vis_data[:-1, :, 1], axis=1)

        output_file["Data"].create_dataset("Left Power time average", data=leftave)
        output_file["Data"].create_dataset("Right Power time average", data=rightave)
        output_file["Data"].copy(input_file["Data/Timestamps"], "Timestamps")
        output_file.copy(input_file["MetaData"], "MetaData")
        output_file.copy(input_file["Markup"], "Markup")

        print "Done."

