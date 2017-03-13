#!/usr/bin/env python

"""
Author: John D. Anderson
Email: jander43@vols.utk.edu
Description: Command line tool for converting data to HDF5
Usage: hdfconv netlogo_table <file> [progbar]
"""

# libs
import csv
import h5py
from tqdm import tqdm


# classes
class HDFConv(object):
    """Class to convert data to HDF5."""

    def _hdf5_path(self, hdfpath):
        """Function to generate path and filename for HDF5 file."""
        argname = hdfpath.split('/')
        csvname = argname[len(argname)-1].rsplit('.', 1)
        h5name = csvname[0] + ".hdf5"
        pathname = ''
        for i, folder in enumerate(argname):
            if i == len(argname)-1:
                return pathname + h5name
            pathname += folder + '/'

    def _csv_linesum(self, fname):
        """Function to count lines in CSV file."""
        with open(fname, 'rU') as f:
            for i, __ in enumerate(f):
                pass
            return i+1

    def netlogo_table(self, fpath, progbar=None):
        """Function to convert CSV data to HDF5."""
        # check pbar
        if progbar is None:
            # disable progbar
            progbar = True
            # set length to zero
            flen = 0
        else:
            # enable progbar
            progbar = False
            # get length of file
            flen = self._csv_linesum(fpath) - 7

        # lists/dicts/containers for data
        atlst = []
        datasets = {}

        # getting path/name of hdf5 file
        self.h5fl = self._hdf5_path(fpath)

        # open "TABLE" csv file and copy to HDF5 file
        with open(fpath, 'rU') as csvfile,  h5py.File(self.h5fl, 'w') as hdf5:

            # read CSV header
            for i, line in enumerate(csv.reader(csvfile)):

                # pulling dataset names and attributes
                if i == 6:
                    # get splitting index
                    index = line.index('[step]')
                    # get attributes
                    atlst = line[:index]
                    # get data set names
                    dname = line[index+1:]
                    # gen dset dict
                    datasets = {d: index+1+i for i, d in enumerate(dname)}
                    # break out
                    break

            # read CSV body
            for line in tqdm(csv.reader(csvfile), total=flen, disable=progbar):
                # group already exists
                if line[0] in hdf5:
                    # get group
                    grp = hdf5[line[0]]
                    # increase data set size
                    for dset_name in grp:
                        dset = grp[dset_name]
                        newsize = dset.len()
                        dset.resize((newsize+1, 2))
                        dset[newsize, 0] = int(line[index])
                        dset[newsize, 1] = float(line[datasets[dset_name]])

                    # go to top of loop
                    continue

                # else create new group
                grp = hdf5.create_group(line[0])

                # fill the attributes
                for attr, row in zip(atlst, line):
                    grp.attrs[attr] = row

                # add initial data
                for name, i in datasets.iteritems():
                    inlist = [float(line[index]), float(line[i])]
                    grp.create_dataset(name, (1, 2), maxshape=(None, 2),
                                       data=inlist)

        # on completion print to stdout
        if progbar:
            print self.h5fl


# executable
if __name__ == '__main__':

    # get fire
    import fire

    # exec
    fire.Fire(HDFConv)
