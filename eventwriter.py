import quakeml

class QuakeMlWriter():
    def __init__(self, filename, provider='GFZ'):
        self.filename = filename
        self.provider = provider

    def write(self, data):
        qml = quakeml.events2quakeml(data, provider=self.provider)
        with open(self.filename, 'w') as f:
            f.write(qml)
