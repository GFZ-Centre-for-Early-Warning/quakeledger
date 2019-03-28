import pandas
import os
import lxml.etree as le
import quakeml
import disaggregation_oq_sources as dos
import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dbvalparaiso import Valparaiso

#DUMMY DATA STUFF SHOULD BE CHANGED AS SOON AS STORAGE ETC IS FINALLY DECIDED
def connect(provider='GFZ'):
    '''
    connects to service
    '''
    filepath=os.path.dirname(__file__)
    filename = os.path.join(filepath,'sqlite3.db')
    engine = create_engine('sqlite:///' + filename)
    if provider=='GFZ':
        return engine

#FUNCTIONS
def convert_360(lon):
    '''
    convert a longitude specified with 180+
    '''
    return lon-360

def filter_spatial(query,lonmin=-180,lonmax=180,latmin=-90,latmax=90,zmin=0,zmax=999):
    '''
    filters spatial
    '''
    query = query.filter(Valparaiso.longitude >= lonmin)
    query = query.filter(Valparaiso.longitude <= lonmax)
    query = query.filter(Valparaiso.latitude >= latmin)
    query = query.filter(Valparaiso.latitude <= latmax)
    query = query.filter(Valparaiso.depth >= zmin)
    query = query.filter(Valparaiso.depth <= zmax)

    return query

def filter_type(query,etype,probability):
    '''
    filters event type and probability
    '''
    #NOTE: probability has no effect currently on historic+expert event filters
    if etype in ['expert','observed']:
        return query.filter_by(type_=etype)
    elif etype in ['stochastic']:
        #return db[(db.type==etype) & (abs(db.probability-p) < 10**-5)]
        query = query.filter_by(type_ = etype)
        query = query.filter_by(probability > probability)
        return query
    elif etype in ['deaggregation']:
        #get stochastic events
        return query.filter_by(type_ = 'stochastic')

def filter_magnitude(query,mmin,mmax):
    '''
    filters magnitude
    '''
    query = query.filter(Valparaiso.magnitude >= mmin)
    query = query.filter(Valparaiso.magnitude <= mmax)
    return query

#QUERY
def query_events(engine, num_events = -1, lonmin=-180,lonmax=180,latmin=-90,latmax=90,mmin=0,mmax=12,zmin=0,zmax=999,p=0,tlat=0,tlon=0,etype='stochastic'):
    '''
    Returns set of events
    type can be:
        -observed (returns set of observed events, probability is rate of event)
        -stochastic (returns stochastic set of events, probability is rate of event)
        -expert     (returns expert defined events, probability is rate of event)
        -deaggregation (returns events matching deaggregation, probability is exceedance probability of hazard curve fur 50 years at target
                        --> requires to define a target)

    Optional Constraints
        - num_events: number of events to be returned. Default -1 i.e. all available events
        - target: tlat,tlon (for deaggregation)
        - event location region: lonmin,lonmax,latmin,latmax (default:-180,180,-90,90)
        - minimum magnitude: mmin (Mw, default:0)
        - maximum magnitude: mmax (Mw, default:12)
        - maximum depth: zmax (km, default 999)
        - probability: p (interpretation depends on type see above)
    '''
    #filter type and probability
    Session = sessionmaker(bind=engine)
    session = Session()
    query = session.query(Valparaiso)    
    query = filter_type(query,etype,p)

    # in the original script there the order
    # is first to match disaggregation
    # and then use all the other filters
    # (spatial and magnitude)
    # 
    # however, to get better performance
    # I try to switch the ordering
    # because in most cases there should be no
    # big difference in the location before
    # and after binning

    #convert 360 degree longitude in case
    if lonmin > 180:
        lonmin = convert_360(lonmin)
    if lonmax > 180:
        lonmax = convert_360(lonmax)

    #spatial filter
    query = filter_spatial(query,lonmin,lonmax,latmin,latmax,zmin,zmax)

    #magnitude filter
    query = filter_magnitude(query,mmin,mmax)
    #sort according to magnitude
    query = query.order_by(Valparaiso.magnitude.desc())
    selected = pandas.read_sql(query.statement, query.session.bind)
    selected.fillna(value=pandas.np.nan, inplace=True)

    #deaggregation
    if etype == 'deaggregation':
        #get events matching deaggregation for target
        selected = dos.match_disaggregation(selected,tlat,tlon,p, session)


    #filter according to num_events
    if (num_events > 0 ):
        selected = selected.iloc[0:num_events]

    #convert to quakeml
    selected=quakeml.events2quakeml(selected,provider='GFZ')

    return selected


def main():
    #Program execution
    con = connect()

#    #test query params
#    lonmin=288
#    lonmax=292
#    latmin=-70
#    latmax=-10
#    mmin=6.6
#    mmax=8.5,
#    zmin=5,
#    zmax=140,
#    tlon=-71.5730623712764
#    tlat=-33.1299174879672
#    #p=0,
#    #etype='observed'
#    etype='deaggregation'
#    #etype='stochastic'
    etype='expert'
#    #poe='likely',
#    #p=0.0659340659
#    #p=0
#    p=0.1 #deaggregation PSHA 10% within 50 years

    lonmin=float(sys.argv[1])
    lonmax=float(sys.argv[2])
    latmin=float(sys.argv[3])
    latmax=float(sys.argv[4])
    mmin=float(sys.argv[5])
    mmax=float(sys.argv[6])
    zmin=float(sys.argv[7])
    zmax=float(sys.argv[8])
    p=float(sys.argv[9])
    etype=sys.argv[10]
    tlon=float(sys.argv[11])
    tlat=float(sys.argv[12])

    selected = query_events(con,lonmin=lonmin,lonmax=lonmax,latmin=latmin,latmax=latmax,mmin=mmin,mmax=mmax,zmin=zmin,zmax=zmax,p=p,tlat=tlat,tlon=tlon,etype=etype)
    ##selected = query_events(db,p=p,etype=etype)
    #
    #test writing
    with open('test.xml','w') as f:
        f.write(selected)

if __name__ =='__main__':
    main()
