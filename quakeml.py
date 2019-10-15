#####################################
# Convert quakeml catalogs to pandas
# and vice versa
import pandas
import lxml.etree as le
import math

# TODO: publicID in quakeml should refer to webservice address
# TODO: add second nodal plane!?

ID_PREFIX = 'quakeml:quakeledger/'


def event2utc(event):
    '''
    given event returns UTC string
    '''
    d=event.fillna(0)
    return '{:04d}-{:02d}-{:02d}T{:02d}:{:02d}:{:09f}Z'.format(int(d.year), int(max(d.month, 1)), int(max(d.day, 1)), int(d.hour), int(d.minute), d.second)


def add_id_prefix(element):
    '''
    Adds an id prefix if necessary.
    '''
    if element.startswith(ID_PREFIX):
        return element
    return ID_PREFIX + element

def utc2event(utc):
    '''
    given utc string returns list with year,month,day,hour,minute,second
    '''
    # last part usually either Z(ulu) or UTC, if not fails
    if utc[-3:]=='UTC':
        utc=utc[:-2]
    elif utc[-1:]=='Z':
        pass
    else:
        raise Exception('Cannot handle timezone other than Z(ulu) or UTC: {}'.format(utc))
    date, time = utc.split('T')
    return [int(v) if i<5 else float(v) for i, v in enumerate([int(d) for d in date.split('-')]+[float(t) for t in time[:-1].split(':')])]


def add_uncertain_child(parent, childname, value, uncertainty):
    '''
    Adds an uncertain child with value/uncertainty pair
    '''
    child = le.SubElement(parent, childname)
    v = le.SubElement(child, 'value')
    v.text = str(value)
    if v.text == 'nan':
        v.text = 'NaN'
    u = le.SubElement(child, 'uncertainty')
    u.text = str(uncertainty)
    return parent


def format_xsdouble(value):
    '''
    Converts the value for a xsdouble field
    to a number or NaN.
    '''
    if value is None or math.isnan(value):
        return 'NaN'

    return str(value)


def events2quakeml(catalog, provider='GFZ'):
    '''
    Given a pandas dataframe with events returns QuakeML version of
    the catalog
    '''
    xml_namespace = 'http://quakeml.org/xmlns/bed/1.2'
    quakeml = le.Element('eventParameters', xmlns=xml_namespace, publicID=add_id_prefix('0'))
    # go through all events
    for i in range(len(catalog)):
        quake = catalog.iloc[i]
        event = le.SubElement(quakeml, 'event', {'publicID': add_id_prefix(str(quake.eventID))})
        preferredOriginID = le.SubElement(event, 'preferredOriginID')
        preferredOriginID.text = add_id_prefix(str(quake.eventID))
        preferredMagnitudeID = le.SubElement(event, 'preferredMagnitudeID')
        preferredMagnitudeID.text = add_id_prefix(str(quake.eventID))
        qtype = le.SubElement(event, 'type')
        qtype.text = 'earthquake'
        description = le.SubElement(event, 'description')
        text = le.SubElement(description, 'text')
        text.text = str(quake.type)
        # origin
        origin = le.SubElement(event, 'origin', {'publicID': add_id_prefix(str(quake.eventID))})
        origin = add_uncertain_child(origin, childname='time', value=event2utc(quake), uncertainty=format_xsdouble(quake.timeUncertainty))
        # time = le.SubElement(origin, 'time')
        # value = le.SubElement(time, 'value')
        # value.text = event2utc(quake)
        # uncertainty = le.SubElement(time, 'uncertainty')
        # uncertainty.text = str(quake.timeUncertainty)
        origin = add_uncertain_child(origin, childname='latitude', value=str(quake.latitude), uncertainty=format_xsdouble(quake.latitudeUncertainty))
        # latitude = le.SubElement(origin, 'latitude')
        # value = le.SubElement(latitude, 'value')
        # value.text = str(quake.latitude)
        # uncertainty = le.SubElement(latitude, 'uncertainty')
        # uncertainty.text = str(quake.sigmaLatitude)
        origin = add_uncertain_child(origin, childname='longitude', value=str(quake.longitude), uncertainty=format_xsdouble(quake.longitudeUncertainty))
        # longitude = le.SubElement(origin, 'longitude')
        # value = le.SubElement(longitude, 'value')
        # value.text = str(quake.longitude)
        # uncertainty = le.SubElement(longitude, 'uncertainty')
        # uncertainty.text = str(quake.sigmaLongitude)
        origin = add_uncertain_child(origin, childname='depth', value=str(quake.depth), uncertainty=format_xsdouble(quake.depthUncertainty))
        # depth = le.SubElement(origin, 'depth')
        # value = le.SubElement(depth, 'value')
        # value.text = str(quake.depth)
        # uncertainty = le.SubElement(depth, 'uncertainty')
        # uncertainty.text = str(quake.depthUncertainty)
        creationInfo = le.SubElement(origin, 'creationInfo')
        author = le.SubElement(creationInfo, 'author')
        author.text = provider
        # originUncertainty
        originUncertainty = le.SubElement(origin, 'originUncertainty')
        # NOTE: imo this should be decided during processing and not on data level --> NOT included
        # preferredDescription = le.SubElement(originUncertainty, 'originUncertainty')
        # preferredDescription.text = quake.preferredOriginUncertainty
        horizontalUncertainty = le.SubElement(originUncertainty, 'horizontalUncertainty')
        horizontalUncertainty.text = format_xsdouble(quake.horizontalUncertainty)
        minHorizontalUncertainty = le.SubElement(originUncertainty, 'minHorizontalUncertainty')
        minHorizontalUncertainty.text = format_xsdouble(quake.minHorizontalUncertainty)
        maxHorizontalUncertainty = le.SubElement(originUncertainty, 'maxHorizontalUncertainty')
        maxHorizontalUncertainty.text = format_xsdouble(quake.maxHorizontalUncertainty)
        azimuthMaxHorizontalUncertainty = le.SubElement(originUncertainty, 'azimuthMaxHorizontalUncertainty')
        azimuthMaxHorizontalUncertainty.text = format_xsdouble(quake.azimuthMaxHorizontalUncertainty)
        # magnitude
        magnitude = le.SubElement(event, 'magnitude', {'publicID': add_id_prefix(str(quake.eventID))})
        magnitude = add_uncertain_child(magnitude, childname='mag', value=str(quake.magnitude), uncertainty=format_xsdouble(quake.magnitudeUncertainty))
        # mag = le.SubElement(magnitude, 'mag')
        # value = le.SubElement(mag, 'value')
        # value.text = str(quake.magnitude)
        # uncertainty = le.SubElement(magnitude, 'uncertainty')
        # uncertainty.text = str(quake.magnitudeUncertainty)
        mtype = le.SubElement(magnitude, 'type')
        mtype.text = 'MW'
        creationInfo = le.SubElement(magnitude, 'creationInfo')
        author = le.SubElement(creationInfo, 'author')
        author.text = provider
        # plane (write only fault plane not auxilliary)
        focalMechanism = le.SubElement(event, 'focalMechanism', {'publicID': add_id_prefix(str(quake.eventID))})
        nodalPlanes = le.SubElement(focalMechanism, 'nodalPlanes', {'preferredPlane': '1'})
        nodalPlane1 = le.SubElement(nodalPlanes, 'nodalPlane1')
        nodalPlane1 = add_uncertain_child(nodalPlane1, childname='strike', value=str(quake.strike), uncertainty=format_xsdouble(quake.strikeUncertainty))
        # strike = le.SubElement(nodalPlane1, 'strike')
        # value  = le.SubElement(strike, 'value')
        # value.text = str(quake.strike)
        # uncertainty  = le.SubElement(strike, 'uncertainty')
        # uncertainty.text = str(quake.sigmaStrike)
        nodalPlane1 = add_uncertain_child(nodalPlane1, childname='dip', value=str(quake.dip), uncertainty=format_xsdouble(quake.dipUncertainty))
        # dip = le.SubElement(nodalPlane1, 'dip')
        # value  = le.SubElement(dip, 'value')
        # value.text = str(quake.dip)
        # uncertainty  = le.SubElement(dip, 'uncertainty')
        # uncertainty.text = str(quake.sigmaDip)
        nodalPlane1 = add_uncertain_child(nodalPlane1, childname='rake', value=str(quake.rake), uncertainty=format_xsdouble(quake.rakeUncertainty))
        # rake = le.SubElement(nodalPlane1, 'rake')
        # value  = le.SubElement(rake, 'value')
        # value.text = str(quake.rake)
        # uncertainty  = le.SubElement(rake, 'uncertainty')
        # uncertainty.text = str(quake.sigmaRake)

    # return str(le.tostring(quakeml, pretty_print=True, xml_declaration=True), encoding='utf-8')
    return le.tostring(quakeml, pretty_print=True, encoding='unicode')


def get_uncertain_child(parent, childname):
    '''
    Given a childname returns value and uncertainty
    '''
    try:
        value = float(parent.find(childname).findtext(add_namespace('value')))
    except:
        value = float('NAN')
    try:
        uncertainty = float(parent.find(childname).findtext(add_namespace('uncertainty')))
    except:
        uncertainty = float('NAN')
    return [value, uncertainty]

def add_namespace(element):
    '''
    Adds the namespace to the quakeml xml elements.
    '''
    return '{http://quakeml.org/xmlns/bed/1.2}' + element

def quakeml2events(quakemlfile, provider='GFZ'):
    '''
    Given a quakeml file/or string returns a pandas dataframe
    '''
    # TODO: add uncertainty
    try:
        # read quakeml catalog
        with open(quakemlfile, 'r') as f:
            quakeml = f.read()
    except:
        # maybe already string
        quakeml = quakemlfile

    quakeml = le.fromstring(quakeml)
    # initialize catalog
    index = [i for i in range(len(quakeml))]
    columns=['eventID', 'Agency', 'Identifier', 'year', 'month', 'day', 'hour', 'minute', 'second', 'timeUncertainty', 'longitude', 'longitudeUncertainty', 'latitude', 'latitudeUncertainty', 'horizontalUncertainty', 'maxHorizontalUncertainty', 'minHorizontalUncertainty', 'azimuthMaxHorizontalUncertainty', 'depth', 'depthUncertainty', 'magnitude', 'magnitudeUncertainty', 'rake', 'rakeUncertainty', 'dip', 'dipUncertainty', 'strike', 'strikeUncertainty', 'type', 'probability']
    catalog=pandas.DataFrame(index=index, columns=columns)
    # add individual events to catalog
    for i, event in enumerate(quakeml):
        # get ID
        catalog.iloc[i].eventID = event.attrib['publicID']
        # type
        catalog.iloc[i].type = event.find(add_namespace('description')).findtext(add_namespace('text'))
        # origin
        origin = event.find(add_namespace('origin'))
        # time
        catalog.iloc[i].year, catalog.iloc[i].month, catalog.iloc[i].day, catalog.iloc[i].hour, catalog.iloc[i].minute, catalog.iloc[i].second = utc2event(
            origin.find(add_namespace('time')).findtext(add_namespace('value')))
        catalog.iloc[i].timeUncertainty = float(origin.find(add_namespace('time')).findtext(add_namespace('uncertainty')))
        # latitude/longitude/depth
        catalog.iloc[i].latitude, catalog.iloc[i].latitudeUncertainty = get_uncertain_child(origin, add_namespace('latitude'))
        catalog.iloc[i].longitude, catalog.iloc[i].longitudeUncertainty = get_uncertain_child(origin, add_namespace('longitude'))
        catalog.iloc[i].depth, catalog.iloc[i].depthUncertainty = get_uncertain_child(origin, add_namespace('depth'))
        # agency/provider
        catalog.iloc[i].agency = origin.find(add_namespace('creationInfo')).findtext(add_namespace('value'))
        # magnitude
        magnitude = event.find(add_namespace('magnitude'))
        catalog.iloc[i].magnitude, catalog.iloc[i].magnitudeUncertainty = get_uncertain_child(magnitude, add_namespace('mag'))
        # originUncertainty
        originUncertainty = origin.find(add_namespace('originUncertainty'))
        catalog.iloc[i].horizontalUncertainty = originUncertainty.find(add_namespace('horizontalUncertainty')).findtext(add_namespace('value'))
        catalog.iloc[i].minHorizontalUncertainty = originUncertainty.find(add_namespace('minHorizontalUncertainty')).findtext(add_namespace('value'))
        catalog.iloc[i].maxHorizontalUncertainty = originUncertainty.find(add_namespace('maxHorizontalUncertainty')).findtext(add_namespace('value'))
        catalog.iloc[i].horizontalUncertainty = originUncertainty.find(add_namespace('azimuthMaxHorizontalUncertainty')).findtext(add_namespace('value'))

        # plane
        nodalPlanes = event.find(add_namespace('focalMechanism')).find(add_namespace('nodalPlanes'))
        preferredPlane = nodalPlanes.get('preferredPlane')
        preferredPlane = nodalPlanes.find(add_namespace('nodalPlane' + preferredPlane))
        # GET uncertain child!!
        catalog.iloc[i].strike, catalog.iloc[i].strikeUncertainty = get_uncertain_child(preferredPlane, add_namespace('strike'))
        catalog.iloc[i].dip   , catalog.iloc[i].dipUncertainty  = get_uncertain_child(preferredPlane, add_namespace('dip'))
        catalog.iloc[i].rake  , catalog.iloc[i].rakeUncertainty = get_uncertain_child(preferredPlane, add_namespace('rake'))
        # catalog.iloc[i].strike = float(nodalPlanes.find(preferredPlane).find('strike').findtext('value'))
        # catalog.iloc[i].dip = float(nodalPlanes.find(preferredPlane).find('dip').findtext('value'))
        # catalog.iloc[i].rake = float(nodalPlanes.find(preferredPlane).find('rake').findtext('value'))

    return catalog
