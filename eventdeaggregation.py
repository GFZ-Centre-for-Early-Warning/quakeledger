#!/usr/bin/env python3

'''
Classes used for the deaggregation
'''

import numpy as np


class DeaggregationResult():
    '''
    Class to contain the deaggration result.
    Gives access to another filtering
    before giving back an pandas dataframe
    '''

    def __init__(self, data):
        self.data = data

    def add_filter_spatial(self, lonmin, lonmax, latmin, latmax, zmin, zmax):
        '''
        Adds a spatial filter on the internal data
        Gives no result back
        '''
        self.data = self.data[
            (self.data.longitude >= lonmin) &
            (self.data.longitude <= lonmax) &
            (self.data.latitude >= latmin) &
            (self.data.latitude <= latmax) &
            (self.data.depth >= zmin) &
            (self.data.depth <= zmax)
        ]

    def add_filter_magnitude(self, mmin, mmax):
        '''
        Adds a filter for the magnitude
        Gives no result back
        '''
        self.data = self.data[
            (self.data.magnitude >= mmin) &
            (self.data.magnitude <= mmax)
        ]

    def add_ordering_magnitude_desc(self):
        '''
        Changes the ordering of the internal data
        to have to events with the highest magnitude first.
        Gives no result back.
        '''
        self.data = self.data.sort_values('magnitude', ascending=False)

    def get_result(self):
        '''
        Returns the result as a pandas dataframe
        '''
        return self.data

class DeaggregationMatcher():
    '''
    Class to do the deaggregation matching
    '''
    def __init__(self, deagg_data, p_lon, p_lat, p_mag):
        self.deagg_data = deagg_data
        self.p_lon = p_lon
        self.p_lat = p_lat
        self.p_mag = p_mag
        self.deagg_data_p_gr_0 = self.deagg_data[self.deagg_data.poe > 0]

    def match_deaggregation(self, event_data):
        '''
        Matches the event data to the deaggregtion data
        returns a DeaggregationResult instance
        '''
        bins = self._binning_xyz(event_data[['longitude', 'latitude', 'magnitude']])
        idx, poe = self._choose_random_events(bins, seed=42)
        matches = event_data.loc[idx].copy()
        matches['probability'] = poe
        return DeaggregationResult(matches)

    def _choose_random_events(self, bins, seed=42):
        '''
        Chooses some events randomly if they their
        binning is near anoth to the deaggration data
        returns the indexes of the data
        and the probability from the deaggregation data
        '''
        idxs = []
        poe = []
        #go through bins of deaggregation
        for i in range(len(self.deagg_data_p_gr_0)):
            seed += i
            row = self.deagg_data_p_gr_0.iloc[i]
            #get events
            matches = DeaggregationMatcher._find_matches(bins, row)
            #append single random sampled idx
            n_matches = len(matches)
            if n_matches > 0:
                # if there are some
                # just take one randomly
                np.random.seed(seed)
                idx = np.random.randint(0, n_matches, 1)[0]
                idxs.append(matches.iloc[idx].name)
                poe.append(row.poe)

        return idxs, poe

    @staticmethod
    def _find_matches(bins, row):
        '''
        Returns all those matches that are very near to the
        bins on terms of spatial near and with the same magnitude
        '''
        return bins[
            (abs(bins.longitude - row.Lon) < 10**-5) &
            (abs(bins.latitude  - row.Lat) < 10**-5) &
            (abs(bins.magnitude - row.Mag) < 10**-5)
        ]



    def _binning_xyz(self, data):
        '''
        Returns the spatial position according the the
        precisions of the deaggregation data
        '''
        xyz = data.copy()
        cols = xyz.columns

        #rounds to bin precision
        xyz[cols[0]] = xyz[cols[0]] / self.p_lon
        xyz[cols[1]] = xyz[cols[1]] / self.p_lat
        xyz[cols[2]] = xyz[cols[2]] / self.p_mag
        xyz = xyz.round()
        xyz[cols[0]] = xyz[cols[0]] * self.p_lon
        xyz[cols[1]] = xyz[cols[1]] * self.p_lat
        xyz[cols[2]] = xyz[cols[2]] * self.p_mag
        return xyz

class DeaggregationAnalyzer():
    '''
    Class to analyse the deaggregation data
    '''

    def __init__(self, data):
        self.data = data

    def _get_precision_lon(self):
        return round(min(np.diff(self.data.Lon.unique())), 5)

    def _get_precision_lat(self):
        return round(min(np.diff(self.data.Lat.unique())), 5)

    def _get_precision_mag(self):
        return round(min(np.diff(self.data.Mag.unique())), 5)

    def get_precisions_lon_lat_mag(self):
        '''
        Returns a tuple with the precisions on longitude, latitude
        and magnitude (in this ordering)
        '''
        return (
            self._get_precision_lon(),
            self._get_precision_lat(),
            self._get_precision_mag()
        )
