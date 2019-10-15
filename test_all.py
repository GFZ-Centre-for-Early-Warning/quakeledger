#!/usr/bin/env python3

'''
Tests for quakeledger
(unit tests and overall
runs with xml validation).
'''

import os
import subprocess
import unittest
import lxml.etree as le


class RunQuakeledgerMixin():
    '''
    Mixin for all the commands that are necessary
    to run quakeledger as a command line tool
    and to test run the validation using python.
    '''

    def _get_as_valid_quakeml(self, filename):
        '''
        Test if a file is valid quakeml.
        '''
        schema = le.XMLSchema(
            le.parse(
                self._get_file_in_project_folder('QuakeML-BED-1.2.xsd')
            )
        )
        parser = le.XMLParser(schema=schema)
        # if there is a problem it will throw an exception
        # and the test will fail
        output = le.parse(filename, parser=parser)
        return output

    def _get_file_in_project_folder(self, inner_filename):
        '''
        Returns the filename relativ to the project folder.
        '''
        directory = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(directory, inner_filename)

    def _clear_output(self):
        '''
        Removes the output file if it is necessary.
        '''
        output_filename = self._get_file_in_project_folder('test.xml')
        if os.path.exists(output_filename):
            os.unlink(output_filename)

    def _validate_output_is_quakeml(self):
        '''
        Validates just the test.xml, which is the output name
        of quakeledger.
        '''
        output_filename = self._get_file_in_project_folder('test.xml')
        return self._get_as_valid_quakeml(output_filename)

    def _run_quakeledger(
            self,
            lonmin,
            lonmax,
            latmin,
            latmax,
            magmin,
            magmax,
            depthmin,
            depthmax,
            propability,
            etype,
            tlon,
            tlat):
        '''
        Runs the quakeledger as a command line tool.
        '''
        subprocess.run(
            [
                'python3',
                'eventquery.py',
                str(lonmin),
                str(lonmax),
                str(latmin),
                str(latmax),
                str(magmin),
                str(magmax),
                str(depthmin),
                str(depthmax),
                str(propability),
                str(etype),
                str(tlon),
                str(tlat)
            ],
            # if the program fails
            # this will complain and the test will fail
            # as well
            check=True,
            cwd=self._get_file_in_project_folder('.'),
        )

    def _run_quakeledger_and_validate(
            self,
            lonmin,
            lonmax,
            latmin,
            latmax,
            magmin,
            magmax,
            depthmin,
            depthmax,
            propability,
            etype,
            tlon,
            tlat):
        '''
        Runs the quakeledger as command line tool.
        First it removes an old output file,
        then it runs the the validation.
        '''
        self._clear_output()
        self._run_quakeledger(
            lonmin=lonmin,
            lonmax=lonmax,
            latmin=latmin,
            latmax=latmax,
            magmin=magmin,
            magmax=magmax,
            depthmin=depthmin,
            depthmax=depthmax,
            propability=propability,
            etype=etype,
            tlon=tlon,
            tlat=tlat,
        )
        return self._validate_output_is_quakeml()


class TestAll(unittest.TestCase, RunQuakeledgerMixin):
    '''
    Test class to run.
    '''

    def test_run_of_quakeledger_and_validation_chile_deaggregation(self):
        '''
        The testcase with some default values for
        chile.
        '''
        eventlist = self._run_quakeledger_and_validate(
            lonmin=288,
            lonmax=292,
            latmin=-70,
            latmax=-10,
            magmin=6.6,
            magmax=8.5,
            depthmin=5,
            depthmax=140,
            propability=0.1,
            etype='deaggregation',
            tlon=-71.5730623712764,
            tlat=-33.1299174879672,
        )
        events = self._get_events(eventlist)
        self.assertLess(0, len(events))

    def test_run_of_quakeledger_and_validation_peru_observed(self):
        '''
        The testcase with some default values for
        peru with the observed ones.
        '''
        eventlist = self._run_quakeledger_and_validate(
            lonmin=276,
            lonmax=292,
            latmin=-20,
            latmax=0,
            magmin=6.6,
            magmax=9.5,
            depthmin=2,
            depthmax=240,
            propability=0.1,
            etype='observed',
            tlon=-71.5730623712764,
            tlat=-33.1299174879672,
        )

        events = self._get_events(eventlist)
        self.assertLess(0, len(events))

    def test_run_of_quakeledger_and_validation_peru_stochastic(self):
        '''
        The testcase with some default values for
        peru with the stochastic ones.
        '''
        eventlist = self._run_quakeledger_and_validate(
            lonmin=276,
            lonmax=292,
            latmin=-20,
            latmax=0,
            magmin=6.6,
            magmax=9.5,
            depthmin=2,
            depthmax=240,
            # lower propability to filter here
            propability=0.05,
            etype='stochastic',
            tlon=-71.5730623712764,
            tlat=-33.1299174879672,
        )

        events = self._get_events(eventlist)
        self.assertLess(0, len(events))

    def _get_events(self, eventlist):
        return eventlist.findall('{http://quakeml.org/xmlns/bed/1.2}event')


if __name__ == '__main__':
    unittest.main()
