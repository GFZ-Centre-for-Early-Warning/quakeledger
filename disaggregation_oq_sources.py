#Given a stochastic set of ruptures as exported from OQ and a (mean_disagg.csv) disaggregation result
#(processed using the /home/mhaas/RIESGOS/disaggregation/createPlot.py routine)
#returns the stochastic set of events associated with the poe of the disaggregation bin it belongs to
import os
import pandas
import numpy as np
from dbsites import Site
from dbmeandisagg import MeanDisagg

def binning_xyz(data,px,py,pz):
    '''
    given pandas data frame (data x,y,z) and bins dy,dy,dz
    returns binned data
    '''
    xyz=data.copy()
    cols=xyz.columns
    #rounds to bin precision
    xyz[cols[0]] = xyz[cols[0]]/px
    xyz[cols[1]] = xyz[cols[1]]/py
    xyz[cols[2]] = xyz[cols[2]]/pz
    xyz = xyz.round()
    xyz[cols[0]] = xyz[cols[0]]*px
    xyz[cols[1]] = xyz[cols[1]]*py
    xyz[cols[2]] = xyz[cols[2]]*pz
    return xyz

#FIXME: Add uncertainty here, i.e., calculate sigmas from all matching events/or just use half bins
def return_random_event(events,disagg,seed=42):
    '''
    Per unique bin returns index of single random event and poe for corresponding disaggregation bin (which have assigned bins)
    '''
    idxs = []
    poe = []
    #go through bins of disaggregation
    for i in range(len(disagg)):
        seed+=i
        row = disagg.iloc[i]
        #get events
        matches = events[(abs(events.longitude-row.Lon)<10**-5)&(abs(events.latitude-row.Lat)<10**-5)&(abs(events.magnitude-row.Mag)<10**-5)]
        #append single random sampled idx
        n=len(matches)
        if n>0:
            np.random.seed(seed)
            idx=np.random.randint(0,n,1)[0]
            idxs.append(matches.iloc[idx].name)
            poe.append(row.poe)

    return [idxs,poe]

def find_nearest_site(sites, lon, lat):
    dists = np.sqrt((sites.lon - lon)**2 + (sites.lat - lat)**2)
    ind = dists.idxmin()
    return sites.iloc[ind]

def match_disaggregation(ruptures,lat,lon,poe,session):
    '''
    Given a set of ruptures, a target with longitude/latitude,
    and a target exceedance probability (e.g., 0.1 = 10%) for 50 years return period
    picks up corresponding deaggregation and selects a single random event
    from the rupture for each bin
    '''
    #read deaggregation sites
    sites_query = session.query(Site)
    sites = pandas.read_sql(sites_query.statement, sites_query.session.bind)

    #find closest match to target
    nearest_site = find_nearest_site(sites, lon, lat)
    slon = nearest_site.lon
    slat = nearest_site.lat
    sid = nearest_site.sid

    mean_disagg_query = session.query(MeanDisagg)
    mean_disagg_query = mean_disagg_query.filter_by(sid=sid)
    mean_disagg_query = mean_disagg_query.filter_by(poe50y=poe)
    dr = pandas.read_sql(mean_disagg_query.statement, mean_disagg_query.session.bind)

    #determine precision
    plon = round(min(np.diff(dr.Lon.unique())),5)
    plat = round(min(np.diff(dr.Lat.unique())),5)
    pmag = round(min(np.diff(dr.Mag.unique())),5)
    #bin the ruptures
    bins = binning_xyz(ruptures[['longitude','latitude','magnitude']],plon,plat,pmag)
    #take only those with non-zero poe
    dr = dr[dr.poe>0]

    #select events
    idxs,poe = return_random_event(bins,dr,seed=42)
    matches = ruptures.loc[idxs]
    matches['probability']=poe

    return matches
