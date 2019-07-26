"""
Used to test that a file can be loaded into mantid from the input directory and
output to the output directory
"""
# pylint:skip-file
import sys
import os

from mantid.simpleapi import SaveNexusProcessed, Load


def reduce(input_file, output_dir):
    ws = Load(input_file)
    file_name = os.path.join(output_dir, 'load-successful.nxs')
    SaveNexusProcessed(InputWorkspace=ws,
                       Filename=file_name)


if __name__ == "__main__":
    reduce(sys.argv[1], sys.argv[2])
