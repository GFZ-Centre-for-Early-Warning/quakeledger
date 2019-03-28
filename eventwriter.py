#!/usr/bin/env python3

'''
Classes to write the event data to files
'''

import quakeml

class QuakeMLWriter():
    '''
    Class to write the event data as quakeml file
    '''
    def __init__(self, filename, provider='GFZ'):
        self._filename = filename
        self._provider = provider

    def write(self, data):
        '''Writes the data to a file'''
        qml = quakeml.events2quakeml(data, provider=self._provider)
        with open(self._filename, 'w') as file_out:
            file_out.write(qml)
