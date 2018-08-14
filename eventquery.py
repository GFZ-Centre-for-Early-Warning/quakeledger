import pandas
import obspy
import obspyDMT

#DUMMY DATA STUFF SHOULD BE CHANGED AS SOON AS STORAGE ETC IS FINALLY DECIDED
#FIXME:currently only csv
def read_database(conn):
    '''
    get data from database
    '''
    return pandas.read_csv(conn)

def connect(provider='GFZ'):
    '''
    connects to service
    '''
    if provider=='GFZ':
        return read_database("example_event_db.csv")

#FUNCTIONS
def convert_360(lon):
    '''
    convert a longitude specified with 180+
    '''
    return lon-360

def filter_spatial(db,lonmin=-180,lonmax=180,latmin=-90,latmax=90,zmin=0,zmax=999):
    '''
    filters spatial
    '''
    return db[(db.longitude >= lonmin) & (db.longitude <= lonmax) & (db.latitude >= latmin) & (db.latitude <= latmax) & (db.depth >= zmin) & (db.depth <= zmax)]

def filter_type(db,etype,probability):
    '''
    filters event type and probability
    '''
    if etype in ['historic','stochastic','historic','deaggregation']:
        return db[(db.type==etype) & (db.probability > p)]
    elif etype in ['expert']:
        return db[(db.type==etype) & (db.probability==p)]

def filter_magnitude(db,mmin,mmax):
    '''
    filters magnitude
    '''
    return db[(db.magnitude >= mmin) & (db.magnitude >= mmax)]

def events2quakeml(events):
    '''
    Returns quakeml from pandas event set
    '''
    pass

#QUERY
def query_events(db,lonmin=-180,lonmax=180,latmin=-90,latmax=90,mmin=0,mmax=9,zmin=0,zmax=999,p=0,etype='stochastic'):
    '''
    Returns set of events
    type can be:
        -stochastic (returns stochastic set of events, probability is rate of event)
        -expert     (returns expert defined events, probability is rate of event)
        -psha       (returns events matching ground motion for psha at target, given probability of exceedance)
        -deaggregation (returns events matching deaggregation, probability is lower level of exceedance probability)

    Optional Constraints
        - target: tlat,tlon
        - distance:
        - boundary region: lonmin,lonmax,latmin,latmax (default:-180,180,-90,90)
        - minimum magnitude: mmin (Mw, default:0)
        - maximum depth: zmax (km, default 999)
        - probability: p (interpretation depends on type see above)
    '''
    if etype!='stochastic':
        raise Exception('Not implemented')
    #filter type and probability
    selected = filter_type(db,etype,p)

    #convert 360 degree longitude in case
    if lonmin > 180:
        lonmin = convert_360(lonmin)
    if lonmax > 180:
        lonmax = convert_360(lonmax)

    #spatial filter
    selected = filter_spatial(selected,lonmin,lonmax,latmin,latmax,zmin,zmax)

    #magnitude filter
    selected = filter_magnitude(db,mmin,mmax)

    #TODO convert to quakeml
    selected=events2quakeml(selected)

    return selected

#Program execution
db = connect()

#test query params
lonmin=288
lonmax=292
latmin=-70
latmax=-10
mmin=6.6
mmax=8.5,
zmin=5,
zmax=140,
p=0,
etype='historic'
#etype='deaggregation'
#etype='stochastic'
#etype='expert'
#poe='likely',

selected = query_events(db,lonmin,lonmax,latmin,latmax,mmin,mmax,zmin,zmax,poe,etype)

