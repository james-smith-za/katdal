################################################################################
# Copyright (c) 2011-2016, National Research Foundation (Square Kilometre Array)
#
# Licensed under the BSD 3-Clause License (the "License"); you may not use
# this file except in compliance with the License. You may obtain a copy
# of the License at
#
#   https://opensource.org/licenses/BSD-3-Clause
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""Container for categorical (i.e. non-numerical) sensor data and related tools."""

import collections
import cPickle as pickle

import numpy as np


class HashableValueWrapper(object):
    """Wrapper that ensures objects are hashable and comparable by value.

    This wrapper class has two main benefits:

      - It prevents sensor values that are NumPy ndarrays themselves (or
        array-like objects such as tuples and lists) from dissolving and
        losing their identity when they are assembled into an array.

      - It ensures that all sensor values become hashable by value, which eases
        comparison (e.g. avoiding the array-valued booleans resulting from
        ndarray comparisons without resorting to np.array_equal) and also
        allows sensor values to be looked up in mappings to simplify indexing.

    Parameters
    ----------
    hashable_value : object that is hashable
        The wrapped version of a sensor value - if not available, use static
        method :meth:`wrap` instead (current implementation expects a pickle)

    Note
    ----
    The current implementation uses pickle to make generic objects hashable,
    even though it is not recommended - see e.g.
    http://www.aminus.org/blogs/index.php/2007/11/03/pickle_dumps_not_suitable_for_hashing
    The main problem is with unordered data like dicts and sets, where the item
    order in the pickle string may differ even though the objects are equal;

      pickle.dumps({1: 0, 9: 0}) == pickle.dumps({9: 0, 1: 0})

    evaluates to False, even though the dicts are equal. On top of this there
    is also the issue of potentially differing pickle protocols.

    """
    def __init__(self, hashable_value):
        self.hashable_value = hashable_value

    def __repr__(self):
        """Short human-friendly string representation of wrapper object."""
        return "<katdal.%s { %r } at 0x%x>" % \
               (self.__class__.__name__, self.unwrap(), id(self))

    def __str__(self):
        """Longer human-friendly string representation of wrapped object."""
        return str(self.unwrap())

    def __eq__(self, other):
        """Equality comparison operator."""
        if not isinstance(other, HashableValueWrapper):
            other = HashableValueWrapper.wrap(other)
        return self.hashable_value == other.hashable_value

    def __ne__(self, other):
        """Inequality comparison operator."""
        return not self == other

    def __lt__(self, other):
        """Less-than comparison operator."""
        raise TypeError("HashableValueWrapper objects are considered unorderable")

    def __gt__(self, other):
        """Greather-than comparison operator."""
        raise TypeError("HashableValueWrapper objects are considered unorderable")

    def __le__(self, other):
        """Less-than-or-equal comparison operator."""
        raise TypeError("HashableValueWrapper objects are considered unorderable")

    def __ge__(self, other):
        """Greater-than-or-equal comparison operator."""
        raise TypeError("HashableValueWrapper objects are considered unorderable")

    def __hash__(self):
        """Return hash value of wrapped object (guaranteed to be hashable)."""
        return hash(self.hashable_value)

    @staticmethod
    def wrap(value):
        """Wrap generic object to become hashable (pickle implementation)."""
        return HashableValueWrapper(pickle.dumps(value))

    def unwrap(self):
        """Extract original object from wrapper (pickle implementation)."""
        return pickle.loads(self.hashable_value)


def unique_in_order(elements, unwrap=False, return_inverse=False):
    """Extract unique elements from *elements* while preserving original order.

    Parameters
    ----------
    elements : sequence of hashables
        Sequence of hashable objects (i.e. objects supporting __hash__ operator)
    unwrap : {False, True}, optional
        If True, unwrap any objects wrapped in a :class:`HashableValueWrapper`
    return_inverse : {False, True}, optional
        If True, also return sequence of indices that can be used to reconstruct
        original `elements` sequence via `[unique_elements[i] for i in inverse]`

    Returns
    -------
    unique_elements : list
        List of unique objects, in original order they were found in `elements`
    inverse : array of int, optional
        If `return_inverse` is True, sequence of indices that can be used to
        reconstruct original sequence

    """
    # Surprisingly, a zero generator like itertools.repeat does not buy you anything
    lookup = collections.OrderedDict(zip(elements, len(elements) * [0]))
    for index, element in enumerate(lookup):
        lookup[element] = index
    if unwrap:
        def maybe_unwrap(v):   # noqa: E301
            return v.unwrap() if isinstance(v, HashableValueWrapper) else v
        unique_elements = [maybe_unwrap(element) for element in lookup]
    else:
        unique_elements = lookup.keys()
    if return_inverse:
        inverse = np.array([lookup[element] for element in elements], dtype=np.int)
        return unique_elements, inverse
    else:
        return unique_elements


# -------------------------------------------------------------------------------------------------
# -- CLASS :  CategoricalData
# -------------------------------------------------------------------------------------------------


class CategoricalData(object):
    """Container for categorical (i.e. non-numerical) sensor data.

    This container allows simple manipulation and interpolation of a time series
    of non-numerical data represented as discrete events. The data is stored as
    a list of sensor values and two integer arrays:

    * `unique_values` stores one copy of each unique object in the data series
    * `events` stores the time indices (dumps) where each event occurs
    * `indices` stores indices linking each event to the `unique_values` list

    The __getitem__ interface (i.e. `data[dump]`) returns the data associated
    with the last event before the requested dump(s), in effect doing a
    zeroth-order interpolation of the data at each event. Events can be added
    and removed and realigned, and the container can be split along the time
    axis, amongst other functionality.

    Parameters
    ----------
    sensor_values : sequence, length *N*
        Sequence of sensor values (of any type, preferably not None [see Notes])
    events : sequence of non-negative ints, length *N* + 1
        Corresponding monotonic sequence of dump indices where each sensor value
        came into effect. The last event is one past the last dump where the
        final sensor value applied, and therefore equal to the total number of
        dumps for which sensor values were specified.

    Attributes
    ----------
    unique_values : list, length *M*
        List of unique sensor values in order they were found in `sensor_values`
    indices : array of int, shape (*N*,)
        Array of indices into `unique_values`, one per sensor event
    dtype : :class:`numpy.dtype` object
        Sensor data type as NumPy dtype (found on demand from `unique_values`)

    Notes
    -----
    It is discouraged to have a sensor value of None as this value is given
    a special meaning in methods such as :meth:`CategoricalData.add` and
    :meth:`sensor_to_categorical`. On the other hand, it is the most sensible
    dummy object value and any Nones entering through this initialiser will
    probably not cause any issues.

    It is better to make `unique_values` a list instead of an array because an
    array assimilates objects such as tuples, lists and other arrays. The
    alternative is an array of :class:`HashableValueWrapper` objects but these
    then need to be unpacked at some later stage which is also tricky.

    """
    def __init__(self, sensor_values, events):
        self.unique_values, self.indices = unique_in_order(sensor_values, unwrap=True,
                                                           return_inverse=True)
        self.events = np.asarray(events)

    @property
    def _hashable_values(self):
        """Hashable version of unique values, wrapping any objects."""
        if self.dtype == np.object:
            return [HashableValueWrapper.wrap(value) for value in self.unique_values]
        else:
            return self.unique_values

    def _lookup(self, dumps):
        """Look up relevant indices occurring at specified *dumps*.

        Parameters
        ----------
        dumps : int or sequence of int
            Specified dumps

        Returns
        -------
        indices : int or array of int
            Corresponding sensor value indices at specified dumps

        """
        preceding_events = self.events.searchsorted(dumps, side='right') - 1
        if np.any(preceding_events < 0) or np.any(preceding_events >= len(self.indices)):
            raise IndexError('Some dumps in (%s) are outside event range: %d <= dumps < %d' %
                             (dumps, self.events[0], self.events[-1]))
        return self.indices[preceding_events]

    def __getitem__(self, key):
        """Look up sensor value at selected dumps.

        Parameters
        ----------
        key : int or slice or sequence of int or sequence of bool
            Index or slice specification selecting certain dumps

        Returns
        -------
        val : object or list of objects
            Sensor values at selected dumps, either single value or list of them

        """
        if isinstance(key, slice):
            # Convert slice notation to the corresponding sequence of dump indices
            key = range(*key.indices(self.events[-1]))
        # Convert sequence of bools (one per dump) to sequence of indices where key is True
        elif np.asarray(key).dtype == np.bool and len(np.asarray(key)) == self.events[-1]:
            key = np.nonzero(key)[0]
        indices = self._lookup(key)
        # Interpret indices as either a sequence of ints or a single int
        try:
            return [self.unique_values[index] for index in indices]
        except TypeError:
            return self.unique_values[indices]

    def __repr__(self):
        """Short human-friendly string representation of categorical data object."""
        return "<katdal.CategoricalData events=%d values=%d type='%s' at 0x%x>" % \
               (len(self.indices), len(self.unique_values), self.dtype, id(self))

    def __str__(self):
        """Long human-friendly string representation of categorical data object."""
        index_width = len(str(self.events[-1] - 1))
        return '\n'.join([('%*d - %*d: %s' % (index_width, segm.start, index_width, segm.stop - 1, val))
                          for segm, val in self.segments()])

    def __len__(self):
        """Length operator indicates number of events produced by sensor."""
        return len(self.indices)

    def _bool_per_dump(self, bool_per_value):
        """Turn list of bools per unique value into an array of bools per dump."""
        bool_per_event = np.atleast_1d(np.array(bool_per_value)[self.indices])
        bool_per_dump = np.empty(self.events[-1], dtype=np.bool)
        for n, (start, end) in enumerate(zip(self.events[:-1], self.events[1:])):
            bool_per_dump[start:end] = bool_per_event[n]
        return bool_per_dump

    def __eq__(self, other):
        """Equality comparison operator."""
        return self._bool_per_dump([value == other for value in self._hashable_values])

    def __ne__(self, other):
        """Inequality comparison operator."""
        return self._bool_per_dump([value != other for value in self._hashable_values])

    def __lt__(self, other):
        """Less-than comparison operator."""
        return self._bool_per_dump([value < other for value in self._hashable_values])

    def __gt__(self, other):
        """Greather-than comparison operator."""
        return self._bool_per_dump([value > other for value in self._hashable_values])

    def __le__(self, other):
        """Less-than-or-equal comparison operator."""
        return self._bool_per_dump([value <= other for value in self._hashable_values])

    def __ge__(self, other):
        """Greater-than-or-equal comparison operator."""
        return self._bool_per_dump([value >= other for value in self._hashable_values])

    @property
    def dtype(self):
        """Sensor value type."""
        if self.unique_values:
            # Put all values into array to find maximum length of any string type
            test_data = np.array(self.unique_values)
            # Beware array-valued sensors; treat their values as opaque objects
            # This forces sensor values to be 1-D at all times (an invariant)
            return test_data.dtype if test_data.ndim == 1 else np.object
        else:
            return np.object

    def segments(self):
        """Generator that iterates through events and returns segment and value.

        Yields
        ------
        segment : slice object
            The slice representing range of dump indices of the current segment
        value : object
            Sensor value associated with segment

        """
        for start, end, ind in zip(self.events[:-1], self.events[1:], self.indices):
            yield slice(start, end), self.unique_values[ind]

    def add(self, event, value=None):
        """Add or override sensor event.

        This adds a new event to the container, with a new value or a duplicate
        of the existing value at that dump. If the new event coincides with an
        existing one, it overrides the value at that dump.

        Parameters
        ----------
        event : int
            Dump of event to add or override
        value : object, optional
            New value for event (duplicate current value at this dump by default)

        """
        # If value has not been seen before, add it to unique_values (and create new index for it)
        if value is not None:
            try:
                value_index = self._hashable_values.index(value)
            except ValueError:
                value_index = len(self.unique_values)
                self.unique_values += [value]
        else:
            value_index = self._lookup(event)
        # If new event coincides with existing event, simply change value of that event, else insert new event
        event_index = self.events.searchsorted(event)
        before, after = event_index, (event_index + 1 if self.events[event_index] == event else event_index)
        self.indices = np.r_[self.indices[:before], [value_index], self.indices[after:]]
        self.events = np.r_[self.events[:before], [event], self.events[after:]]

    def remove(self, value):
        """Remove sensor value, remapping indices and merging segments in process.

        If the sensor value does not exist, do nothing.

        Parameters
        ----------
        value : object
            Sensor value to remove from container

        """
        try:
            index = self._hashable_values.index(value)
        except ValueError:
            pass
        else:
            keep = (self.indices != index)
            remap = np.arange(len(self.unique_values))
            remap[index:] -= 1
            self.indices = remap[self.indices[keep]]
            self.events = np.r_[self.events[keep], self.events[-1]]
            del self.unique_values[index]

    def add_unmatched(self, segments, match_dist=1):
        """Add duplicate events for segment starts that don't match sensor events.

        Given a sequence of segments, this matches each segment start to the
        nearest sensor event dump (within *match_dist*). Any unmatched segment
        starts are added as duplicate sensor events (or ignored if they fall
        outside the sensor event range).

        Parameters
        ----------
        segments : sequence of int
            Monotonically increasing sequence of segment starts, including an
            extra element at the end that is one past the end of the last segment
        match_dist : int, optional
            Maximum distance in dumps that signify a match between events

        """
        # Identify unmatched segment starts
        segments = np.asarray(segments)
        unmatched = segments[np.abs(self.events[np.newaxis, :] - segments[:, np.newaxis]).min(axis=1) > match_dist]
        # Add these dumps as duplicate events, ignoring those that are out of bounds
        for segm in unmatched:
            try:
                self.add(segm)
            except IndexError:
                pass

    def align(self, segments):
        """Align sensor events with segment starts, possibly discarding events.

        Given a sequence of segments, this moves each sensor event dump onto the
        nearest segment start. If more than one event ends up in the same segment,
        only keep the last event, discarding the rest.

        The end result is that the sensor event dumps become a subset of the
        segment starts and there cannot be more sensor events than segments.

        Parameters
        ----------
        segments : sequence of int
            Monotonically increasing sequence of segment starts, including an
            extra element at the end that is one past the end of the last segment

        """
        # For each event, pick the segment with the closest start to it and then shift event onto segment start
        segments_with_event = np.abs(self.events[np.newaxis, :] - segments[:, np.newaxis]).argmin(axis=0)
        events = segments[segments_with_event]
        # When multiple sensor events are associated with the same segment, only keep the final one
        final = np.nonzero(np.diff(events) > 0)[0]
        subset, self.indices = np.unique(self.indices[final], return_inverse=True)
        self.unique_values = [self.unique_values[index] for index in subset]
        self.events = np.r_[events[final], events[-1]]

    def partition(self, segments):
        """Partition dataset into multiple sets along time axis.

        Given a sequence of segments, split the container into a sequence of
        containers, one per segment. Each container contains only the events
        occurring within its corresponding segment, with event dumps relative to
        the start of the segment, and the containers share the same unique
        values.

        Parameters
        ----------
        segments : sequence of int
            Monotonically increasing sequence of segment starts, including an
            extra element at the end that is one past the end of the last segment

        Returns
        -------
        split_data : sequence of :class:`CategoricalData` objects
            Resulting multiple datasets in chronological order

        """
        # Ignore last element in event list, as it is not a real event but a placeholder for dataset length
        events = self.events[:-1]
        # Find segment starts in event sequence, associating dumps before first event with it, ditto for ones past last
        initial_indices = self.indices[(events.searchsorted(segments[:-1], side='right') - 1).clip(0, len(events) - 1)]
        split_data = []
        for start, end, initial_index in zip(segments[:-1], segments[1:], initial_indices):
            segment_events = (events >= start) & (events < end)
            # Bypass the normal CategoricalData initialiser to ensure that each cat_data has the same unique_values
            cat_data = CategoricalData([], [])
            cat_data.unique_values = self.unique_values
            cat_data.indices = self.indices[segment_events]
            cat_data.events = events[segment_events] - start
            # Insert initial event if it is not there, and pad events with data segment length
            if len(cat_data.events) == 0 or cat_data.events[0] != 0:
                cat_data.indices = np.r_[initial_index, cat_data.indices]
                cat_data.events = np.r_[0, cat_data.events, end - start]
            else:
                cat_data.events = np.r_[cat_data.events, end - start]
            split_data.append(cat_data)
        return split_data

    def remove_repeats(self):
        """Remove repeated events of the same value."""
        changes = np.nonzero([1] + np.diff(self.indices).tolist())[0]
        self.indices = self.indices[changes]
        self.events = np.r_[self.events[changes], self.events[-1]]

# -------------------------------------------------------------------------------------------------
# -- Utility functions
# -------------------------------------------------------------------------------------------------


def concatenate_categorical(split_data, **kwargs):
    """Concatenate multiple categorical datasets into one along time axis.

    Join a sequence of categorical datasets together, by forming a common set of
    unique values, remapping events to these and incrementing the event dumps of
    each dataset to start off where the previous dataset ended.

    Parameters
    ----------
    split_data : sequence of :class:`CategoricalData` objects
        Sequence of containers to concatenate

    Returns
    -------
    data : :class:`CategoricalData` object
        Concatenated dataset

    """
    if len(split_data) == 1:
        return split_data[0]
    # Synthesise segment starts from the time length of each dataset
    segments = np.cumsum([0] + [cat_data.events[-1] for cat_data in split_data])
    data = CategoricalData([], [])
    # Combine all unique values in the order they are found in datasets
    split_values = [cat_data._hashable_values for cat_data in split_data]
    inverse_splits = np.cumsum([0] + [len(vals) for vals in split_values])
    data.unique_values, inverse = unique_in_order(sum(split_values, []),
                                                  unwrap=True, return_inverse=True)
    indices, events = [], []
    for n, cat_data in enumerate(split_data):
        # Remap indices to new unique_values array
        lookup = np.array(inverse[inverse_splits[n]:inverse_splits[n + 1]])
        indices.append(lookup[cat_data.indices])
        # Offset events by the start of each segment
        events.append(cat_data.events[:-1] + segments[n])
    # Add overall time length as the final event
    events.append([segments[-1]])
    data.indices = np.concatenate(indices)
    data.events = np.concatenate(events)
    if not kwargs.get('allow_repeats', False):
        data.remove_repeats()
    return data


def sensor_to_categorical(sensor_timestamps, sensor_values, dump_midtimes, dump_period,
                          greedy_values=None, initial_value=None, transform=None, allow_repeats=False, **kwargs):
    """Align categorical sensor events with dumps and clean up spurious events.

    This converts timestamped sensor data into a categorical dataset by
    comparing the sensor timestamps to a series of dump timestamps and assigning
    each sensor event to the dump in which it occurred. When multiple sensor
    events happen in the same dump, only the last one is kept. The first dump is
    guaranteed to have a valid value by either using the supplied `initial_value`
    or extrapolating the first proper value back in time. The sensor data may
    be transformed before events that repeat values are potentially discarded.
    Finally, events with values marked as "greedy" swallow the last dump of their
    preceding events, where the sensor value is changing into the greedy value.
    A greedy value therefore takes precedence over another value when both occur
    within the same dump (either changing from or to the greedy value).

    Parameters
    ----------
    sensor_timestamps : sequence of float, length *M*
        Sequence of sensor timestamps (typically UTC seconds since Unix epoch)
    sensor_values : sequence, length *M*
        Corresponding sequence of sensor values
    dump_midtimes : sequence of float, length *N*
        Sequence of dump midtimes (same reference as sensor timestamps)
    dump_period : float
        Duration of each dump, in seconds
    greedy_values : sequence or None, optional
        List of (transformed) sensor values considered "greedy"
    initial_value : object or None, optional
        Sensor value to use for dump = 0 up to the first proper event
        (the default is to force the first proper event to start at dump = 0)
    transform : callable or None, optional
        Transform sensor values before discarding repeats and applying greed
    allow_repeats : {False, True}, optional
        If False, discard sensor events that do not change (transformed) value

    Returns
    -------
    data : :class:`CategoricalData` object
        Constructed categorical dataset

    """
    sensor_timestamps = np.atleast_1d(sensor_timestamps)
    sensor_values = np.atleast_1d(sensor_values)
    dump_midtimes = np.atleast_1d(dump_midtimes)
    # Check if sensor values are objects wrapped in HashableValueWrappers
    wrapped_values = len(sensor_values) and isinstance(sensor_values[0],
                                                       HashableValueWrapper)
    # Convert sensor event times to dump indices (pick the dump during which each sensor event occurred)
    # The last event is fixed at one-past-the-last-dump, to indicate the end of the last segment
    events = np.r_[dump_midtimes.searchsorted(sensor_timestamps - 0.5 * dump_period), len(dump_midtimes)]
    # Cull any empty segments (i.e. when multiple sensor events occur within a single dump, only keep last one)
    # This also gets rid of excess events before the first dump and after the last dump
    non_empty = np.nonzero(np.diff(events) > 0)[0]
    sensor_values, events = sensor_values[non_empty], np.r_[events[non_empty], len(dump_midtimes)]
    # Force first dump to have valid sensor value (use initial value or first proper value advanced to start)
    if events[0] != 0 and initial_value is not None:
        if wrapped_values:
            initial_value = HashableValueWrapper.wrap(initial_value)
        sensor_values, events = np.r_[[initial_value], sensor_values], np.r_[0, events]
    events[0] = 0
    # Apply optional transform to sensor values
    if transform is not None:
        if wrapped_values:
            unwrapped_transform = transform
            transform = lambda value: HashableValueWrapper.wrap(unwrapped_transform(value.unwrap()))   # noqa: E731
        sensor_values = np.array([transform(y) for y in sensor_values])
    # Discard sensor events that do not change the (transformed) sensor value (i.e. that repeat the previous value)
    if not allow_repeats:
        changes = [n for n in range(len(sensor_values)) if (n == 0) or (sensor_values[n] != sensor_values[n - 1])]
        sensor_values, events = sensor_values[changes], np.r_[events[changes], len(dump_midtimes)]
    # Extend segments with "greedy" values to include first dump of next segment where sensor value is changing
    if greedy_values is not None:
        if wrapped_values:
            greedy_values = [HashableValueWrapper.wrap(value) for value in greedy_values]
        greedy_to_nongreedy = [n for n in range(1, len(sensor_values))
                               if sensor_values[n - 1] in greedy_values and sensor_values[n] not in greedy_values]
        events[greedy_to_nongreedy] += 1
    return CategoricalData(sensor_values, events)
