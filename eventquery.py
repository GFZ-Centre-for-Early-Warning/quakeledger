#!/usr/bin/env/python

"""
Quakeledger
-----------------

Command line program to query data from a catalog.
"""

import argparse
import os

from eventdataprovider import Database
from eventdeaggregation import DeaggregationAnalyzer, DeaggregationMatcher
from eventwriter import QuakeMLWriter


class Main:
    """
    Main class to execute
    """

    def __init__(self, args):
        folder = os.path.dirname(__file__)

        # db to use to query all the data
        self.database = Database.from_local_sql_db(folder, "sqlite3.db")

        # command line arguments
        self.lonmin = args.lonmin
        self.lonmax = args.lonmax
        self.latmin = args.latmin
        self.latmax = args.latmax
        self.mmin = args.mmin
        self.mmax = args.mmax
        self.zmin = args.zmin
        self.zmax = args.zmax
        self.probability = args.p
        self.etype = args.etype
        self.tlon = args.tlon
        self.tlat = args.tlat

        # some possibility to extract only a specific number of events
        self.num_events = -1

        # in case there is some deaggregation necessary
        # precision
        self.precision_lon = 0
        self.precision_lat = 0
        self.precision_mag = 0
        # data for the deaggregation itself
        self.optional_deaggregation_data = None

        # result
        self.selected = None

        self.filename_output = "test.xml"

    def _check_longitude(self):
        """If there is a longitude > 180 than it should be converted"""
        if self.lonmin > 180:
            self.lonmin = Main._convert_360(self.lonmin)
        if self.lonmax > 180:
            self.lonmax = Main._convert_360(self.lonmax)

    @staticmethod
    def _convert_360(lon):
        """
        convert a longitude specified with 180+
        """
        return lon - 360

    def _needs_deaggreation(self):
        return self.etype == "deaggregation"

    def _prepare_deaggreation(self, data_provider):
        """
        Extracts all the necessary data for the deaggregation
        and stores it in the instance variables
        """
        site_provider = data_provider.create_provider_for_sites()
        site = site_provider.get_nearest(self.tlon, self.tlat)
        deagg_provider = data_provider.create_provider_for_mean_deagg()
        self.optional_deaggregation_data = (
            deagg_provider.get_all_for_site_and_poe(site, self.probability)
        )
        deagg_analyzer = DeaggregationAnalyzer(
            self.optional_deaggregation_data
        )
        p_lon, p_lat, p_mag = deagg_analyzer.get_precisions_lon_lat_mag()
        self.precision_lon, self.precision_lat, self.precision_mag = (
            p_lon,
            p_lat,
            p_mag,
        )
        # the reason behind storing this data is,
        # that the deaggregation may change the location of the data
        # so we need the precision of the deaggregated values to
        # extend the bounding box to query the database
        #
        # it there is no need for deaggregation,
        # than the precisions are all zero
        # and they don't influence the database query

    def _deaggregate(self):
        """
        Does the deaggregation.
        Returns a pandas dataframe with the result.
        Because of the deaggregation a bigger extend of data
        must be queried from the database
        (see the method for binning).
        So we need another spatial filtering
        (and filtering for the magnitude as well)
        after deaggregation.
        """
        matcher = DeaggregationMatcher(
            self.optional_deaggregation_data,
            self.precision_lon,
            self.precision_lat,
            self.precision_mag,
        )
        deaggregation_result = matcher.match_deaggregation(self.selected)
        # another filter
        # because the deaggregation has needed a bigger extend of data
        deaggregation_result.add_filter_spatial(
            self.lonmin,
            self.lonmax,
            self.latmin,
            self.latmax,
            self.zmin,
            self.zmax,
        )
        deaggregation_result.add_filter_magnitude(self.mmin, self.mmax)
        deaggregation_result.add_ordering_magnitude_desc()

        return deaggregation_result.get_result()

    def _write_file(self):

        writer = QuakeMLWriter(self.filename_output)
        writer.write(self.selected)

    def _filter_num_events(self):
        """
        If the setting for the number of events is set,
        then only query the first n
        """

        if self.num_events > 0:
            self.selected = self.selected.iloc[0 : self.num_events]

    def run(self):
        """
        Method to:
        - connect with the database
        - query the database
        - do some deaggregation if necessary
        - write the output
        """

        self._check_longitude()

        with self.database as data_provider:

            event_provider = data_provider.create_provider_for_events()
            event_provider.add_filter_type(self.etype, self.probability)

            if self._needs_deaggreation():
                self._prepare_deaggreation(data_provider)

            # if the deaggregation is necessary
            # then the fields for the precision
            # have meaningful values
            #
            # to prepare the deaggregation it is also
            # necessary to provide a bigger extend
            # to search for earth quakes, because
            # the may be binned inside of the area
            # of interest
            event_provider.add_filter_spatial(
                self.lonmin - self.precision_lon,
                self.lonmax + self.precision_lon,
                self.latmin - self.precision_lat,
                self.latmax + self.precision_lat,
                self.zmin,
                self.zmax,
            )
            # same for the magnitude
            event_provider.add_filter_magnitude(
                self.mmin - self.precision_mag, self.mmax + self.precision_mag
            )

            # there should be the ordering
            # with the highest magnitde first
            event_provider.add_ordering_magnitude_desc()

            self.selected = event_provider.get_results()

            if self._needs_deaggreation():
                self.selected = self._deaggregate()

            # because of sorting by magnitde
            # there are only those events with the
            # hightest magnitude
            self._filter_num_events()

            self._write_file()

    @classmethod
    def create_with_arg_parser(cls):
        """
        Creates an arg parser and uses that to create the Main class
        """
        arg_parser = argparse.ArgumentParser(
            description="""Program to query a earth quake catalog"""
        )
        arg_parser.add_argument(
            "lonmin",
            help="Minimal longitude to search for earth quakes",
            type=float,
            default=288.0,
        )
        arg_parser.add_argument(
            "lonmax",
            help="Maximal longitude to search for earth quakes",
            type=float,
            default=292.0,
        )
        arg_parser.add_argument(
            "latmin",
            help="Minimal latitude to search for earth quakes",
            type=float,
            default=-70.0,
        )
        arg_parser.add_argument(
            "latmax",
            help="Maximal latitude to search for earth quakes",
            type=float,
            default=-10.0,
        )
        arg_parser.add_argument(
            "mmin",
            help="Lowest magnitude to search for earth qaukes",
            type=float,
            default=6.6,
        )
        arg_parser.add_argument(
            "mmax",
            help="Hightes magnitude to search for earth quakes",
            type=float,
            default=8.5,
        )
        arg_parser.add_argument(
            "zmin",
            help="Mininal depth to search for earth quakes",
            type=float,
            default=5.0,
        )
        arg_parser.add_argument(
            "zmax",
            help="Maximal depth to search for earth quakes",
            type=float,
            default=140.0,
        )
        arg_parser.add_argument(
            "p", help="Probability for deaggregation", type=float, default=0.1
        )
        arg_parser.add_argument(
            "etype",
            help="Type of the earth quake in the catalog (expert, stochastic, deaggregation)",
            type=str,
            default="deaggregation",
        )
        arg_parser.add_argument(
            "tlon",
            help="Longitude to search for the nearest side on deaggregation",
            type=float,
            default=-71.5730623712764,
        )
        arg_parser.add_argument(
            "tlat",
            help="Latitude to search for the nearest side on deaggregation",
            type=float,
            default=-33.1299174879672,
        )
        args = arg_parser.parse_args()

        return cls(args)


if __name__ == "__main__":
    Main.create_with_arg_parser().run()
