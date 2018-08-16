#####################################
# Convert quakeml catalogs to pandas
# and vice versa
import pandas
import lxml.etree as le

def event2utc(event):
    '''
    given event returns UTC string
    '''
    d=event.fillna(0)
    return '{:04d}-{:02d}-{:02d}T{:02d}:{:02d}:{:09f}Z'.format(d.year,max(d.month,1),max(d.day,1),d.hour,d.minute,d.second)

def utc2event(utc):
    '''
    given utc string returns list with year,month,day,hour,minute,second
    '''
    date,time = utc.split('T')
    return [int(v) if i<5 else float(v) for i,v in enumerate([int(d) for d in date.split('-')]+[float(t) for t in time[:-1].split(':')])]

def events2quakeml(catalog,provider='GFZ'):
    '''
    Given a pandas dataframe with events returns QuakeML version of
    the catalog
    '''
    #TODO: add rupture plane orientation
    xml_namespace = 'http://quakeml.org/xmlns/quakeml/1.2'
    quakeml = le.Element('eventParameters',namespace=xml_namespace)
    for i in range(len(catalog)):
        quake = catalog.iloc[i]
        event = le.SubElement(quakeml,'event',{'publicID':quake.eventID})
        preferredOriginID = le.SubElement(event,'preferredOriginID')
        preferredOriginID.text=quake.eventID
        preferredMagnitudeID = le.SubElement(event,'preferredMagnitudeID')
        preferredMagnitudeID.text=quake.eventID
        qtype = le.SubElement(event,'type')
        qtype.text = 'earthquake'
        description = le.SubElement(event,'description')
        text = le.SubElement(description,'text')
        text.text = quake.type
        origin = le.SubElement(event,'origin',{'publicID':quake.eventID})
        time = le.SubElement(origin,'time')
        value = le.SubElement(time,'value')
        value.text = event2utc(quake)
        latitude = le.SubElement(origin,'latitude')
        value = le.SubElement(latitude,'value')
        value.text = str(quake.latitude)
        longitude = le.SubElement(origin,'longitude')
        value = le.SubElement(longitude,'value')
        value.text = str(quake.longitude)
        depth = le.SubElement(origin,'depth')
        value = le.SubElement(depth,'value')
        value.text = str(quake.depth)
        creationInfo = le.SubElement(origin,'creationInfo')
        author = le.SubElement(creationInfo,'value')
        author.text = provider
        magnitude = le.SubElement(event,'magnitude',{'publicID':quake.eventID})
        mag = le.SubElement(magnitude,'mag')
        value = le.SubElement(mag,'value')
        value.text = str(quake.magnitude)
        mtype = le.SubElement(magnitude,'type')
        mtype.text = 'MW'
        creationInfo = le.SubElement(magnitude,'creationInfo')
        author = le.SubElement(creationInfo,'value')
        author.text = provider

    return le.tostring(quakeml,pretty_print=True,xml_declaration=True,encoding='unicode')


def quakeml2events(quakemlfile,provider='GFZ'):
    '''
    Given a quakeml file returns a pandas dataframe
    '''
    #TODO: add rupture plane orientation
    #read quakeml catalog
    with open(quakemlfile,'r') as f:
        quakeml = f.read()
    quakeml = le.fromstring(quakeml)
    quakeml = le.Element('eventParameters',namespace=xml_namespace)
    #initialize catalog
    index = [i for i in range(len(quakeml))]
    columns=['eventID', 'Agency', 'Identifier', 'year', 'month', 'day', 'hour', 'minute', 'second', 'timeError', 'longitude', 'latitude','SemiMajor90', 'SemiMinor90', 'ErrorStrike', 'depth', 'depthError', 'magnitude', 'sigmaMagnitude', 'type', 'probability', 'fuzzy']
    catalog=pandas.DataFrame(index=index,columns=columns)
    #add individual events to catalog
    for i,event in enumerate(quakeml):
        #get ID
        catalog.iloc[i].eventID = event.attrib['publicID']
        #type
        catalog.iloc[i].type = event.find('description').findtext('text')
        #origin
        origin = event.find('origin')
        #time
        catalog.iloc[i].year,catalog.iloc[i].month,catalog.iloc[i].day,catalog.iloc[i].hour,catalog.iloc[i].minute,catalog.iloc[i].second = utc2event(origin.find('time').findtext('value'))
        #latitude/longitude/depth
        catalog.iloc[i].latitude = float(origin.find('latitude').findtext('value'))
        catalog.iloc[i].longitude = float(origin.find('longitude').findtext('value'))
        catalog.iloc[i].depth = float(origin.find('depth').findtext('value'))
        #agency/provider
        catalog.iloc[i].agency = origin.find('creationInfo').findtext('value')
        #magnitude
        catalog.iloc[i].magnitude = float(event.find('magnitude').find('mag').findtext('value'))

    return catalog
