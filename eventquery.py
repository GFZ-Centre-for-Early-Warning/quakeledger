import pandas
import os
import lxml.etree as le
import quakeml
import disaggregation_oq_sources as dos
import sys
import sqlite3

#DUMMY DATA STUFF SHOULD BE CHANGED AS SOON AS STORAGE ETC IS FINALLY DECIDED
def connect(provider='GFZ'):
    '''
    connects to service
    '''
    filepath=os.path.dirname(__file__)
    filename = os.path.join(filepath,'sqlite3.db')
    con = sqlite3.connect(filename)
    if provider=='GFZ':
        return con

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
    return query + """ 
        and v.longitude >= {lonmin} 
        and v.longitude <= {lonmax} 
        and v.latitude >= {latmin} 
        and v.latitude  <= {latmax} 
        and v.depth >= {zmin} 
        and v.depth <= {zmax}""".format(
                lonmin=float(lonmin), 
                lonmax=float(lonmax), 
                latmin=float(latmin), 
                latmax=float(latmax), 
                zmin=float(zmin), 
                zmax=float(zmax))

def filter_type(query,etype,probability):
    '''
    filters event type and probability
    '''
    #NOTE: probability has no effect currently on historic+expert event filters
    if etype in ['expert','observed']:
        return query + " and v.type = '" + etype + "'"
    elif etype in ['stochastic']:
        #return db[(db.type==etype) & (abs(db.probability-p) < 10**-5)]
        return query + " and v.type = '" + etype + "' and v.probability > " + probability
    elif etype in ['deaggregation']:
        #get stochastic events
        return query + " and v.type = 'stochastic'"

def filter_magnitude(query,mmin,mmax):
    '''
    filters magnitude
    '''
    return query + " and v.magnitude >= {mmin} and v.magnitude <= {mmax}".format(
            mmin=float(mmin),
            mmax=float(mmax))

#QUERY
def query_events(con, num_events = -1, lonmin=-180,lonmax=180,latmin=-90,latmax=90,mmin=0,mmax=12,zmin=0,zmax=999,p=0,tlat=0,tlon=0,etype='stochastic'):
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
    query = '''
        select 
            v.eventID, v.Agency, 
            v.Identifier, v.year, 
            v.month, v.day, 
            v.hour, v.minute, 
            v.second, v.timeUncertainty,
            v.longitude, v.longitudeUncertainty, 
            v.latitude, v.latitudeUncertainty, 
            v.horizontalUncertainty, v.minHorizontalUncertainty,
            v.maxHorizontalUncertainty, v.azimuthMaxHorizontalUncertainty,
            v.depth, v.depthUncertainty, 
            v.magnitude, v.magnitudeUncertainty,
            v.rake, v.rakeUncertainty, 
            v.dip, v.dipUncertainty, 
            v.strike, v.strikeUncertainty,
            v.type, v.probability
        from valparaiso v
        where 1=1
    '''
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
    query = query + " order by v.magnitude desc"
    selected = pandas.read_sql(query, con)

    #deaggregation
    if etype == 'deaggregation':
        #get events matching deaggregation for target
        selected = dos.match_disaggregation(selected,tlat,tlon,p, con)


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
