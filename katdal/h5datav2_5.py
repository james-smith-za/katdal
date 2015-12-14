"""Data accessor class for HDF5 files produced by AVN DBE."""

import logging

import numpy as np
import h5py
import katpoint

from .dataset import DataSet, WrongVersion, BrokenFile, Subarray, SpectralWindow, \
    DEFAULT_SENSOR_PROPS, DEFAULT_VIRTUAL_SENSORS, _robust_target
from .sensordata import SensorData, SensorCache
from .categorical import CategoricalData, sensor_to_categorical
from .lazy_indexer import LazyIndexer, LazyTransform

logger = logging.getLogger(__name__)

# TODO: This was a KAT-7 simplification. Do the same things apply to us? Should probably be discussed.
# Simplify the scan activities to derive the basic state of the antenna (slewing, scanning, tracking, stopped)
SIMPLIFY_STATE = {'scan_ready': 'slew', 'scan': 'scan', 'scan_complete': 'scan', 'track': 'track', 'slew': 'slew'}

SENSOR_PROPS = dict(DEFAULT_SENSOR_PROPS)
# TODO: I don't think we've implemented anything with this name on AVN at all, so we still need to decide.
SENSOR_PROPS.update({
    '*activity': {'greedy_values': ('slew', 'stop'), 'initial_value': 'slew',
                  'transform': lambda act: SIMPLIFY_STATE.get(act, 'stop')},
    '*target': {'initial_value': '', 'transform': _robust_target},
    # These float sensors are actually categorical by nature as they represent user settings
    'RFE/center-frequency-hz': {'categorical': True},
    'RFE/rfe7.lo1.frequency': {'categorical': True},
    '*attenuation': {'categorical': True},
    '*attenuator.horizontal': {'categorical': True},
    '*attenuator.vertical': {'categorical': True},
})

SENSOR_ALIASES = {
    'nd_roach': 'roach.noise.diode.on',
}


def _calc_azel(cache, name, ant):
    """Calculate virtual (az, el) sensors from actual ones in sensor cache."""
    # TODO: This is where the name of the az/el thingies will need to be updated. We may not use the exact same naming convention.
    # Kat-7 stores 6 different AZ/EL values based on requested and actual and pointing models applied, only the final one
    # is actually used to return the az/el values for the katdal object.
    # real_sensor = 'Antennas/%s/%s' % (ant, 'pos.actual-scan-azim' if name.endswith('az') else 'pos.actual-scan-elev')
    real_sensor = 'Antennas/%s/%s' % (ant, 'pos.azim' if name.endswith('az') else 'pos.elev')
    print cache.get(real_sensor)
    cache[name] = sensor_data = katpoint.deg2rad(cache.get(real_sensor))
    return sensor_data

VIRTUAL_SENSORS = dict(DEFAULT_VIRTUAL_SENSORS)
VIRTUAL_SENSORS.update({'Antennas/{ant}/az': _calc_azel, 'Antennas/{ant}/el': _calc_azel})

# TODO: We haven't implemented any flags as such yet. We can do, but then we must decide what to do with them.
FLAG_NAMES = ('reserved0', 'static', 'cam', 'reserved3', 'detected_rfi', 'predicted_rfi', 'reserved6', 'reserved7')
FLAG_DESCRIPTIONS = ('reserved - bit 0', 'predefined static flag list', 'flag based on live CAM information',
                     'reserved - bit 3', 'RFI detected in the online system', 'RFI predicted from space based pollutants',
                     'reserved - bit 6', 'reserved - bit 7')

#--------------------------------------------------------------------------------------------------
#--- Utility functions
#--------------------------------------------------------------------------------------------------


def get_single_value(group, name):
    """Return single value from attribute or dataset with given name in group.

    If `name` is an attribute of the HDF5 group `group`, it is returned,
    otherwise it is interpreted as an HDF5 dataset of `group` and the last value
    of `name` is returned. This is meant to retrieve static configuration values
    that potentially get set more than once during capture initialisation, but
    then does not change during actual capturing.

    Parameters
    ----------
    group : :class:`h5py.Group` object
        HDF5 group to query
    name : string
        Name of HDF5 attribute or dataset to query

    Returns
    -------
    value : object
        Attribute or last value of dataset

    """
    return group.attrs[name] if name in group.attrs else group[name][-1]


def dummy_dataset(name, shape, dtype, value):
    """Dummy HDF5 dataset containing a single value.

    This creates a dummy HDF5 dataset in memory containing a single value. It
    can have virtually unlimited size as the dataset is highly compressed.

    Parameters
    ----------
    name : string
        Name of dataset
    shape : sequence of int
        Shape of dataset
    dtype : :class:`numpy.dtype` object or equivalent
        Type of data stored in dataset
    value : object
        All elements in the dataset will equal this value

    Returns
    -------
    dataset : :class:`h5py.Dataset` object
        Dummy HDF5 dataset

    """
    # It is important to randomise the filename as h5py does not allow two writable file objects with the same name
    # Without this randomness katdal can only open one file requiring a dummy dataset
    random_string = ''.join(['%02x' % (x,) for x in np.random.randint(256, size=8)])
    dummy_file = h5py.File('%s_%s.h5' % (name, random_string), driver='core', backing_store=False)
    return dummy_file.create_dataset(name, shape=shape, maxshape=shape, dtype=dtype, fillvalue=value, compression='gzip')

#--------------------------------------------------------------------------------------------------
#--- CLASS :  H5DataV2_5
#--------------------------------------------------------------------------------------------------


class H5DataV2_5(DataSet):
    """Load HDF5 format version 2.5 file produced by AVN DBE.

    For more information on attributes, see the :class:`DataSet` docstring.

    Parameters
    ----------
    filename : string
        Name of HDF5 file
    ref_ant : string, optional
        Name of reference antenna, used to partition data set into scans
        (default is first antenna in use)
    time_offset : float, optional
        Offset to add to all timestamps, in seconds
    mode : string, optional
        HDF5 file opening mode (e.g. 'r+' to open file in write mode)
    quicklook : {False, True}
        True if synthesised timestamps should be used to partition data set even
        if real timestamps are irregular, thereby avoiding the slow loading of
        real timestamps at the cost of slightly inaccurate label borders
    kwargs : dict, optional
        Extra keyword arguments, typically meant for other formats and ignored

    Attributes
    ----------
    file : :class:`h5py.File` object
        Underlying HDF5 file, exposed via :mod:`h5py` interface
    """
    def __init__(self, filename, ref_ant='', time_offset=0.0, mode='r', quicklook=False, **kwargs):
        DataSet.__init__(self, filename, ref_ant, time_offset)

        # Load file
        self.file, self.version = H5DataV2_5._open(filename, mode)
        f = self.file

        # Load main HDF5 groups
        data_group    = f["Data"]
        config_group  = f["MetaData/Configuration"]
        sensors_group = f["MetaData/Sensors"]
        markup_group  = f["Markup"]

        # Get observation script parameters, with defaults
        for k, v in config_group['Observation'].attrs.iteritems():
            self.obs_params[str(k)] = v
        self.observer      = self.obs_params.get('observer', '')
        self.description   = self.obs_params.get('description', '')
        self.experiment_id = self.obs_params.get('experiment_id', '')

        # ------ Extract timestamps ------
        self.dump_period = get_single_value(config_group['Correlator'], 'int_time')
        # Obtain visibility data and timestamps
        self._vis         = data_group['VisData']
        self._stokes      = data_group['StokesData']
        self._timestamps  = data_group['Timestamps']
        self._time_av_ll = data_group["Left Power time average"]
        self._time_av_rr = data_group["Right Power time average"]
        self._time_av_q  = data_group["Stokes Q time average"]
        self._time_av_u  = data_group["Stokes U time average"]
        num_dumps         = len(self._timestamps)
        if num_dumps     != self._vis.shape[0] - 1:  # This is where a blank row always seems to come through... TODO: Warn Craig about this.
            raise BrokenFile('Number of timestamps received '
                        '(%d) differs from number of dumps in data (%d)' % (num_dumps, self._vis.shape[0]))
        # Do quick test for uniform spacing of timestamps (necessary but not sufficient)
        expected_dumps = (self._timestamps[num_dumps - 1] - self._timestamps[0]) / self.dump_period + 1
        # The expected_dumps should always be an integer (like num_dumps), unless the timestamps and/or dump period
        # are messed up in the file, so the threshold of this test is a bit arbitrary (e.g. could use > 0.5)
        irregular = abs(expected_dumps - num_dumps) >= 0.01
        if irregular:
            # Warn the user, as this is anomalous
            logger.warning(("Irregular timestamps detected in file '%s': "
                           "expected %.3f dumps based on dump period and start/end times, got %d instead") %
                           (filename, expected_dumps, num_dumps))
            if quicklook:
                logger.warning("Quicklook option selected - partitioning data based on synthesised timestamps instead")
        if not irregular or quicklook:
            # Estimate timestamps by assuming they are uniformly spaced (much quicker than loading them from file).
            # This is useful for the purpose of segmenting data set, where accurate timestamps are not that crucial.
            # The real timestamps are still loaded when the user explicitly asks for them.
            data_timestamps = self._timestamps[0] + self.dump_period * np.arange(num_dumps)
        else:
            # Load the real timestamps instead (could take several seconds on a large data set)
            data_timestamps = self._timestamps[:num_dumps]
        # Move timestamps from start of each dump to the middle of the dump
        data_timestamps += 0.5 * self.dump_period + self.time_offset
        if data_timestamps[0] < 1e9:
            logger.warning("File '%s' has invalid first correlator timestamp (%f)" % (filename, data_timestamps[0],))
        self._time_keep = np.ones(num_dumps, dtype=np.bool)
        self.start_time = katpoint.Timestamp(data_timestamps[0] - 0.5 * self.dump_period)
        self.end_time = katpoint.Timestamp(data_timestamps[-1] + 0.5 * self.dump_period)

        # ------ Extract flags ------
        # Check if flag group is present, else use dummy flag data
        self._flags = markup_group['flags'] if 'flags' in markup_group else \
            dummy_dataset('dummy_flags', shape=self._vis.shape[:-1], dtype=np.uint8, value=0)
        # Obtain flag descriptions from file or recreate default flag description table
        self._flags_description = markup_group['flags_description'] if 'flags_description' in markup_group else \
            np.array(zip(FLAG_NAMES, FLAG_DESCRIPTIONS))

        # ------ Extract sensors ------
        # Populate sensor cache with all HDF5 datasets below sensor group that fit the description of a sensor
        cache = {}

        def register_sensor(name, obj):
            """A sensor is defined as a non-empty dataset with expected dtype."""
            if isinstance(obj, h5py.Dataset) and obj.shape != () and obj.dtype.names == ('timestamp', 'value', 'status'):
                cache[name] = SensorData(obj, name)
        sensors_group.visititems(register_sensor)
        # Use estimated data timestamps for now, to speed up data segmentation
        self.sensor = SensorCache(cache, data_timestamps, self.dump_period, keep=self._time_keep,
                                  props=SENSOR_PROPS, virtual=VIRTUAL_SENSORS, aliases=SENSOR_ALIASES)

        # ------ Extract subarrays ------
        # By default, only pick antennas that were in use by the script
        script_ants = config_group['Observation'].attrs['ants'].split(',')
        self.ref_ant = script_ants[0] if not ref_ant else ref_ant
        # Original list of correlation products as pairs of input labels
        single_dish_prods = get_single_value(config_group['Correlator'], 'bls_ordering')
        if len(single_dish_prods) != self._vis.shape[2]:
            raise BrokenFile('Number of data labels (containing expected antenna names) '
                             'received from h5 file (%d) differs from number of baselines in data (%d)' %
                             (len(single_dish_prods), self._vis.shape[2]))
        # All antennas in configuration as katpoint Antenna objects
        ants = []
        for antenna in config_group["Antennas"]:
            name           = config_group["Antennas"][antenna].attrs['name']
            latitude       = config_group["Antennas"][antenna].attrs['latitude']
            longitude      = config_group["Antennas"][antenna].attrs['longitude']
            diameter       = config_group["Antennas"][antenna].attrs['diameter']
            delay_model    = config_group["Antennas"][antenna].attrs['delay_model']
            pointing_model = config_group["Antennas"][antenna].attrs['pointing_model']
            beamwidth      = config_group["Antennas"][antenna].attrs['beamwidth']

            ants.append(katpoint.Antenna(name, latitude, longitude, diameter, delay_model, pointing_model, beamwidth))

        self.subarrays = [Subarray(ants, single_dish_prods)]
        self.sensor['Observation/subarray'] = CategoricalData(self.subarrays, [0, len(data_timestamps)])
        self.sensor['Observation/subarray_index'] = CategoricalData([0], [0, len(data_timestamps)])
        # Store antenna objects in sensor cache too, for use in virtual sensor calculations
        for ant in ants:
            self.sensor['Antennas/%s/antenna' % (ant.name,)] = CategoricalData([ant], [0, len(data_timestamps)])

        # ------ Extract spectral windows / frequencies ------
        centre_freq = self.sensor.get('RFE/center-frequency-hz')
        num_chans = get_single_value(config_group['Correlator'], 'n_chans')
        if num_chans != self._vis.shape[1]:
            raise BrokenFile('Number of channels received from correlator '
                             '(%d) differs from number of channels in data (%d)' % (num_chans, self._vis.shape[1]))
        bandwidth = get_single_value(config_group['Correlator'], 'bandwidth')
        channel_width = bandwidth / num_chans
        sideband = int(config_group["Correlator"].attrs["sideband"])
        # try:
        #     mode = self.sensor.get('DBE/dbe.mode').unique_values[0]  # TODO: Determine what our DBE modes are going to be.
        # except KeyError, IndexError:
        #     # Guess the mode for version 2.0 files that haven't been re-augmented
        #     mode = 'wbc' if num_chans <= 1024 else 'wbc8k' if bandwidth > 200e6 else 'nbc'

        # I added "sideband" here because KAT-7's stuff is always in a lower sideband and therefore spectrally inverted.
        # Ours is not necessarily.
        # self.spectral_windows = [SpectralWindow(spw_centre, channel_width, num_chans, mode)
        #                          for spw_centre in centre_freq.unique_values]
        self.spectral_windows = [SpectralWindow(spw_centre, channel_width, num_chans, sideband=sideband)
                                 for spw_centre in centre_freq.unique_values]

        self.sensor['Observation/spw'] = CategoricalData([self.spectral_windows[idx] for idx in centre_freq.indices],
                                                         centre_freq.events)
        self.sensor['Observation/spw_index'] = CategoricalData(centre_freq.indices, centre_freq.events)

        # ------ Extract scans / compound scans / targets ------
        # Use the activity sensor of reference antenna to partition the data set into scans (and to set their states)
        scan = self.sensor.get('Antennas/%s/activity' % (self.ref_ant,))
        # If the antenna starts slewing on the second dump, incorporate the first dump into the slew too.
        # This scenario typically occurs when the first target is only set after the first dump is received.
        # The workaround avoids putting the first dump in a scan by itself, typically with an irrelevant target.
        if len(scan) > 1 and scan.events[1] == 1 and scan[1] == 'slew':
            scan.events, scan.indices = scan.events[1:], scan.indices[1:]
            scan.events[0] = 0
        # Use labels to partition the data set into compound scans
        label = sensor_to_categorical(markup_group['labels']['timestamp'], markup_group['labels']['label'],
                                      data_timestamps, self.dump_period, **SENSOR_PROPS['Observation/label'])
        # Discard empty labels (typically found in raster scans, where first scan has proper label and rest are empty)
        # However, if all labels are empty, keep them, otherwise whole data set will be one pathological compscan...
        # TODO: Decide whether to keep this check.
        if len(label.unique_values) > 1:
            label.remove('')
        # Create duplicate scan events where labels are set during a scan (i.e. not at start of scan)
        # ASSUMPTION: Number of scans >= number of labels (i.e. each label should introduce a new scan)
        scan.add_unmatched(label.events)
        self.sensor['Observation/scan_state'] = scan
        self.sensor['Observation/scan_index'] = CategoricalData(range(len(scan)), scan.events)
        # Move proper label events onto the nearest scan start
        # ASSUMPTION: Number of labels <= number of scans (i.e. only a single label allowed per scan)
        label.align(scan.events)
        # If one or more scans at start of data set have no corresponding label, add a default label for them
        if label.events[0] > 0:
            label.add(0, '')
        self.sensor['Observation/label'] = label
        self.sensor['Observation/compscan_index'] = CategoricalData(range(len(label)), label.events)
        # Use the target sensor of reference antenna to set the target for each scan
        target = self.sensor.get('Antennas/%s/target' % (self.ref_ant,))
        # Move target events onto the nearest scan start
        # ASSUMPTION: Number of targets <= number of scans (i.e. only a single target allowed per scan)
        target.align(scan.events)
        self.sensor['Observation/target'] = target
        self.sensor['Observation/target_index'] = CategoricalData(target.indices, target.events)
        # Set up catalogue containing all targets in file, with reference antenna as default antenna
        self.catalogue.add(target.unique_values)
        self.catalogue.antenna = self.sensor['Antennas/%s/antenna' % (self.ref_ant,)][0]
        # Ensure that each target flux model spans all frequencies in data set if possible
        self._fix_flux_freq_range()

        # Avoid storing reference to self in transform closure below, as this hinders garbage collection
        dump_period, time_offset = self.dump_period, self.time_offset
        # Restore original (slow) timestamps so that subsequent sensors (e.g. pointing) will have accurate values
        extract_time = LazyTransform('extract_time', lambda t, keep: t + 0.5 * dump_period + time_offset)
        self.sensor.timestamps = LazyIndexer(self._timestamps, keep=slice(num_dumps), transforms=[extract_time])
        # Apply default selection and initialise all members that depend on selection in the process
        self.select(spw=0, subarray=0, ants=script_ants)

    @staticmethod
    def _open(filename, mode='r'):
        """Open file and do basic version and augmentation sanity check."""
        f = h5py.File(filename, mode)
        version = f.attrs.get('version', '1.x')
        if not version == '2.5':
            raise WrongVersion("Attempting to load version '%s' file with version 2.5 loader" % (version,))
        # TODO: decide whether to keep this.
        if 'augment_ts' not in f.attrs:
            raise BrokenFile('HDF5 file not augmented - please run dummy_augment.py in the scripts directory.')

        return f, version

    @staticmethod
    def _get_ants(filename):
        """Quick look function to get the list of antennas in a data file.

        This is intended to be called without createing a full katdal object.

        Parameters
        ----------
        filename : string
            Data file name

        Returns
        -------
        antennas : list of :class:'katpoint.Antenna' objects

        """
        f, version = H5DataV2_5._open(filename)
        config_group = f['MetaData/Configuration']
        all_ants = [ant for ant in config_group['Antennas']]
        script_ants = config_group['Observation'].attrs.get('script_ants')
        script_ants = script_ants.split(',') if script_ants else all_ants
        return [katpoint.Antenna(config_group['Antennas'][ant].attrs['description']) for ant in script_ants if ant in all_ants]

    @staticmethod
    def _get_targets(filename):
        """Quick look function to get the list of targets in a data file.

        This is intended to be called without createing a full katdal object.

        Parameters
        ----------
        filename : string
            Data file name

        Returns
        -------
        targets : :class:'katpoint.Catalogue' object
            All targets in file

        """
        f, version = H5DataV2_5._open(filename)
        # Use the delay-tracking centre as the one and only target
        # Try two different sensors for the DBE target
        try:
            target_list = f['MetaData/Sensors/DBE/target']
        except Exception:
            # Since h5py errors have varied over the years, we need Exception
            target_list = f['MetaData/Sensors/Beams/Beam0/target']
        all_target_strings = [target_data[1] for target_data in target_list]
        return katpoint.Catalogue(np.unique(all_target_strings))

    def __str__(self):
        """Verbose human-friendly string representation of data set."""
        descr = [super(H5DataV2_5, self).__str__()]
        # append the process_log, if it exists, for non-concatenated h5 files
        if 'process_log' in self.file['History']:
            descr.append('-------------------------------------------------------------------------------')
            descr.append('Process log:')
            for proc in self.file['History']['process_log']:
                param_list = '%15s:' % proc[0]
                for param in proc[1].split(','):
                    param_list += '  %s' % param
                descr.append(param_list)
        return '\n'.join(descr)

    @property
    def timestamps(self):
        """Visibility timestamps in UTC seconds since Unix epoch.

        The timestamps are returned as an array indexer of float64, shape (*T*,),
        with one timestamp per integration aligned with the integration
        *midpoint*. To get the data array itself from the indexer `x`, do `x[:]`
        or perform any other form of indexing on it.

        """
        # Avoid storing reference to self in transform closure below, as this hinders garbage collection
        dump_period, time_offset = self.dump_period, self.time_offset
        extract_time = LazyTransform('extract_time', lambda t, keep: t + 0.5 * dump_period + time_offset)  # TODO: Might need to look at this.
        return LazyIndexer(self._timestamps, keep=self._time_keep, transforms=[extract_time])

    @property
    def vis(self):
        """Single-dish observational data, 32-bit signed integer, LL, RR, Q, U.

        This is not strictly speaking visibility data, as AVN DBE produces
        single-dish data, but the name was used to preserve consistency with
        the other formats. It is recorded using 32-bit signed integers, but
        returned as double-precision floating points.
        The shape of the array is (*T*, *F*, *B*), with time along the first
        dimension, frequency along the second dimension. The third dimension
        consists of LL and RR autocorrelation.

        The returned array always has all three dimensions,
        even for scalar (single) values. The number of integrations *T* matches
        the length of :meth:`timestamps`, the number of frequency channels *F*
        matches the length of :meth:`freqs` and the number of correlation
        products *B* matches the length of :meth:`corr_products`. To get the
        data array itself from the indexer `x`, do `x[:]` or perform any other
        form of indexing on it. Only then will data be loaded into memory.
        """
        def _extract_vis(vis, keep):
            # Ensure that keep tuple has length of 3 (truncate or pad with blanket slices as necessary)
            keep = keep[:3] + (slice(None),) * (3 - len(keep))
            # Final indexing ensures that returned data are always 3-dimensional (i.e. keep singleton dimensions)
            # Unlike the KAT-7 stuff, this isn't comlpex at all, just real-valued.
            force_3dim = tuple([(np.newaxis if np.isscalar(dim_keep) else slice(None)) for dim_keep in keep])
            return vis.astype(np.float64)[force_3dim]
        extract_vis = LazyTransform('extract_vis', _extract_vis, lambda shape: shape, np.float64)
        return LazyIndexer(self._vis, transforms=[extract_vis])

    @property
    def stokes(self):
        """Single-dish observational data, 32-bit signed integer, Stokes Q and U. Same
        (*T*, *F*, *B*) arrangement as the vis data.
        """
        def _extract_stokes(stokes, keep):
            keep = keep[:3] + (slice(None),) * (3 - len(keep))
            force_3dim = tuple([(np.newaxis if np.isscalar(dim_keep) else slice(None)) for dim_keep in keep])
            return stokes.astype(np.float64)[force_3dim]
        extract_stokes = LazyTransform('extract_stokes', _extract_stokes, lambda shape: shape, np.float64)
        return LazyIndexer(self._stokes, transforms=[extract_stokes])

    @property
    def time_av_ll(self):
        def _extract_time_av_ll(ll, keep):
            return ll.astype(np.float64)
        extract_time_av_ll = LazyTransform('extract_time_av_ll', _extract_time_av_ll, lambda shape: shape, np.float64)
        return LazyIndexer(self._time_av_ll, transforms=[extract_time_av_ll])

    @property
    def time_av_rr(self):
        def _extract_time_av_rr(rr, keep):
            return rr.astype(np.float64)
        extract_time_av_rr = LazyTransform('extract_time_av_rr', _extract_time_av_rr, lambda shape: shape, np.float64)
        return LazyIndexer(self._time_av_rr, transforms=[extract_time_av_rr])

    @property
    def time_av_q(self):
        def _extract_time_av_q(q, keep):
            return q.astype(np.float64)
        extract_time_av_q = LazyTransform('extract_time_av_q', _extract_time_av_q, lambda shape: shape, np.float64)
        return LazyIndexer(self._time_av_q, transforms=[extract_time_av_q])

    @property
    def time_av_u(self):
        def _extract_time_av_u(u, keep):
            return u.astype(np.float64)
        extract_time_av_u = LazyTransform('extract_time_av_u', _extract_time_av_u, lambda shape: shape, np.float64)
        return LazyIndexer(self._time_av_u, transforms=[extract_time_av_u])

    @property
    def u(self):
        raise NotImplementedError("File is AVN Single-dish format, uv-plane is not applicable.")

    @property
    def v(self):
        raise NotImplementedError("File is AVN Single-dish format, uv-plane is not applicable.")

    @property
    def w(self):
        raise NotImplementedError("File is AVN Single-dish format, uv-plane is not applicable.")

    # TODO: Haven't implemented any flags just yet.
    # def flags(self, names=None):
    #     """Flags as a function of time, frequency and baseline.
    #
    #     Parameters
    #     ----------
    #     names : None or string or sequence of strings, optional
    #         List of names of flags that will be OR'ed together, as a sequence or
    #         a string of comma-separated names (use all flags by default)
    #
    #     Returns
    #     -------
    #     flags : :class:`LazyIndexer` object of bool, shape (*T*, *F*, *B*)
    #         Array indexer with time along the first dimension, frequency along
    #         the second dimension and correlation product ("baseline") index
    #         along the third dimension. To get the data array itself from the
    #         indexer `x`, do `x[:]` or perform any other form of indexing on it.
    #         Only then will data be loaded into memory.
    #
    #     """
    #     names = names.split(',') if isinstance(names, basestring) else FLAG_NAMES if names is None else names
    #
    #     # Create index list for desired flags
    #     flagmask = np.zeros(8, dtype=np.int)
    #     known_flags = [row[0] for row in self._flags_description]
    #     for name in names:
    #         try:
    #             flagmask[known_flags.index(name)] = 1
    #         except ValueError:
    #             logger.warning("'%s' is not a legitimate flag type for this file" % (name,))
    #     # Pack index list into bit mask
    #     flagmask = np.packbits(flagmask)
    #     if not flagmask:
    #         logger.warning('No valid flags were selected - setting all flags to False by default')
    #
    #     def _extract_flags(flags, keep):
    #         # Ensure that keep tuple has length of 3 (truncate or pad with blanket slices as necessary)
    #         keep = keep[:3] + (slice(None),) * (3 - len(keep))
    #         # Final indexing ensures that returned data are always 3-dimensional (i.e. keep singleton dimensions)
    #         force_3dim = tuple([(np.newaxis if np.isscalar(dim_keep) else slice(None)) for dim_keep in keep])
    #         flags_3dim = flags[force_3dim]
    #         # Use flagmask to blank out the flags we don't want
    #         total_flags = np.bitwise_and(flagmask, flags_3dim)
    #         # Convert uint8 to bool: if any flag bits set, flag is set
    #         return np.bool_(total_flags)
    #     extract_flags = LazyTransform('extract_flags', _extract_flags, dtype=np.bool)
    #     return LazyIndexer(self._flags, (self._time_keep, self._freq_keep, self._corrprod_keep),
    #                        transforms=[extract_flags])
