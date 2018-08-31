#Given a stochastic set of ruptures as exported from OQ and a (mean_disagg.csv) disaggregation result
#(processed using the /home/mhaas/RIESGOS/disaggregation/createPlot.py routine)
#returns the stochastic set of events associated with the poe of the disaggregation bin it belongs to
import os
import pandas
import numpy as np
#import scipy
#import time

#t0=time.time()

##OQruptures
#rup = pandas.read_csv("ruptures_3411.csv",skiprows=1,delimiter='\t')
##disaggregation
#dr = pandas.read_csv("mean_disagg_0.1.csv")
#
#def match_row(data,ref):
#    '''
#    given data (x,y,z) matches it to reference (x,y,z)
#    and returns index of row in ref matching row in data
#    columns have to be in same order!
#    NOTE: CHANGED RETURNS POEs
#    '''
#    cd = data.columns
#    cr = ref.columns
#    poes=[]
#    #idxs=[]
#    for i in range(len(data)):
#        row = data.iloc[i]
#        #match
#        try:
#            #idxs.append(ref[(ref[cr[0]]==row[cd[0]])&(ref[cr[1]]==row[cd[1]])&(ref[cr[2]]==row[cd[2]])].index[0])
#            poes.append(float(ref[(ref[cr[0]]==row[cd[0]])&(ref[cr[1]]==row[cd[1]])&(ref[cr[2]]==row[cd[2]])].poe))
#        except:
#            #print(row)
#            #pass
#            poes.append(0.)
#    #return idxs
#    return poes
#
def oqrup2cat(ruptures,dtype='deaggregation',provider='GFZ'):
    '''
    Converts a set of OQ ruptures to a catalog
    '''
    #initialize
    index = [i for i in range(len(ruptures))]
    columns=['eventID', 'Agency', 'Identifier', 'year', 'month', 'day', 'hour', 'minute', 'second', 'timeError', 'longitude', 'latitude','SemiMajor90', 'SemiMinor90', 'ErrorStrike', 'depth', 'depthError', 'magnitude', 'sigmaMagnitude','rake','dip','strike','type', 'probability', 'fuzzy']
    catalog=pandas.DataFrame(index=index,columns=columns)
    #add values
    catalog.eventID   = ruptures.rupid
    catalog.Agency    = provider
    catalog.longitude = ruptures.centroid_lon
    catalog.latitude  = ruptures.centroid_lat
    catalog.depth     = ruptures.centroid_depth
    catalog.magnitude = ruptures.mag
    catalog.type      = dtype
    #not necessarily defined
    try:
        catalog.strike    = ruptures.strike
    except:
        pass
    try:
        catalog.dip       = ruptures.dip
    except:
        pass
    try:
        catalog.rake      = ruptures.rake
    except:
        pass
    try:
        catalog.probability = ruptures.poe
    except:
        pass

    return catalog

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

def match_disaggregation(ruptures,lat,lon,poe):
    '''
    Given a set of ruptures, a target with longitude/latitude,
    and a target exceedance probability (e.g., 0.1 = 10%) for 50 years return period
    picks up corresponding deaggregation and selects a single random event
    from the rupture for each bin
    '''
    #read deaggregation sites
    filepath=os.path.dirname(__file__)
    sites_filename = os.path.join(filepath,"sites.csv")
    sites = pandas.read_csv(sites_filename)

    #find closest match to target
    slon= [sites.iloc[i].lon for i,v in enumerate(sites.lon) if abs(v-lon)==min(abs(sites.lon - lon))][0]
    slon= [sites.iloc[i].lon for i,v in enumerate(sites.lon) if abs(v-lon)==min(abs(sites.lon - lon))][0]
    slat= [sites.iloc[i].lat for i,v in enumerate(sites.lat) if abs(v-lat)==min(abs(sites.lat - lat))][0]
    sid = int(sites[(sites.lon==slon) & (sites.lat==slat)].sid)
    #get deaggregation
    disagg_filename = os.path.join(filepath,"mean_disagg.csv")
    dr = pandas.read_csv(disagg_filename)
    #get that for specified hazard level and site
    dr = dr[(dr.sid==sid) & (dr.poe50y==poe)]
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
    #make sure no index problems for following conversion
    #matches.to_csv('matches.csv',index=False)
    #matches = matches.reset_index()

    ##convert to catalog style
    #catalog = oqrup2cat(matches,provider='GFZ')


    ##save matches
    #catalog.to_csv('catalog.csv',index=False)

#print(time.time()-t0)




##determine precision of disaggregation (up to 5 digits)
#plon = round(min(np.diff(dr.Lon.unique())),5)
#plat = round(min(np.diff(dr.Lat.unique())),5)
#pmag = round(min(np.diff(dr.Mag.unique())),5)
#
##bin the ruptures
#bins = binning_xyz(rup[['centroid_lon','centroid_lat','mag']],plon,plat,pmag)

#associate each event in OQruptures with poe in dr
#rup['poe'] = 0.
#get matches
#poes,idxs = match_row(bins,dr)
#rup['poe'] = match_row(bins,dr)

##TAKE ONLY NON-ZERO
#print('WARNING: CONSIDERING ONLY DISAGGREGATION BINS WITH POE > 0')
#dr = dr[dr.poe>0]
#
##selects events
#idxs,poe = return_random_event(bins,dr,seed=42)
#matches = rup.loc[idxs]
#matches['poe']=poe
##make sure no index problems for following conversion
#matches.to_csv('matches.csv',index=False)
#matches = matches.reset_index()
#
##convert to catalog style
#catalog = oqrup2cat(matches,provider='GFZ')
#
#
##save matches
#catalog.to_csv('catalog.csv',index=False)
#
#print(time.time()-t0)
