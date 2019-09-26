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
    def _is_valid_quakeml(self, filename):
        schema = le.XMLSchema(le.parse(self._get_file_in_project_folder('QuakeML-BED-1.2.xsd')))
        parser = le.XMLParser(schema=schema)
        # if there is a problem it will throw an exception
        # and the test will fail
        output = le.parse(filename, parser=parser)

    def _get_file_in_project_folder(self, inner_filename):
        directory = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(directory, inner_filename)

    def _clear_output(self):
        output_filename = self._get_file_in_project_folder('test.xml')
        if os.path.exists(output_filename):
            os.unlink(output_filename)

    def _validate_output_is_quakeml(self):
        output_filename = self._get_file_in_project_folder('test.xml')
        self._is_valid_quakeml(output_filename)

    def _run_quakeledger(self, lonmin, lonmax, latmin, latmax, magmin, magmax, depthmin, depthmax, propability, etype, tlon, tlat):
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
            check=True,
            cwd=self._get_file_in_project_folder('.'),
        )

    def _run_quakeledger_and_validate(self, lonmin, lonmax, latmin, latmax, magmin, magmax, depthmin, depthmax, propability, etype, tlon, tlat):
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
        self._validate_output_is_quakeml()

class TestAll(unittest.TestCase, RunQuakeledgerMixin):
    '''
    Test class to run.
    '''


    def test_run_of_quakeledger_and_validation_chile(self):
        self._run_quakeledger_and_validate(
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

if __name__ == '__main__':
    unittest.main()

