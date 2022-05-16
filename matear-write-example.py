# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# + [markdown] tags=[]
# # Notebook to read and extract the data for computing transports terms
#
#
# simple analysis of the CAFE60 BGC output - version for the OOD
#
#

# + tags=[]
import xarray as xr
from dask_jobqueue import PBSCluster
from dask.distributed import Client
import numpy as np
#import xrft
import scipy
import matplotlib.pyplot as plt
import datetime
import pandas as pd
import matplotlib.dates as mdates
from matplotlib.dates import DateFormatter
# %config Completer.use_jedi = False

from eofs.xarray import Eof

# + [markdown] tags=[] toc-hr-collapsed=true
# # Start Cluster

# + [markdown] tags=[]
# # non OOD cluster
#
# walltime = '04:00:00'
# cores = 2
# memory = '8GB'
#
# cluster = PBSCluster(walltime=str(walltime), cores=cores, memory=str(memory),
#                      job_extra=['-l ncpus='+str(cores),'-l mem='+str(memory),
#                                 '-l storage=gdata/v14+scratch/v14+gdata/lp01+scratch/lp01+scratch/ux06+gdata/rr3+gdata/al33+gdata/zv2+gdata/xv83'],
#                      header_skip=["select"])
#
# cluster.scale(jobs=6)
# client = Client(cluster)
# client


# + tags=[]
# in the OOD cluster
from dask.distributed import Client,Scheduler
from dask_jobqueue import SLURMCluster
cluster = SLURMCluster(cores=16,memory="47GB")
client = Client(cluster)
cluster.scale(cores=32)

# + tags=[]
client

#cluster.restart()
#print(client)
#client.restart()
# -

print(client)


# + [markdown] tags=[]
# # Read in data
#
# and create useful variables for further calculations 

# + tags=[]
def climatology(dsx,TIME1):
    clim = dsx.groupby(TIME1+'.month').mean(dim=TIME1)
    anom = dsx.groupby(TIME1+'.month') - clim
    season=dsx.groupby(TIME1+'.season').mean(dim=TIME1)
    return(clim,season,anom)


# +
file1='/g/data/xv83/dcfp/CAFE60v1/ocean_month.zarr.zip'
file2='/g/data/xv83/dcfp/CAFE60v1/ocean_bgc_month.zarr.zip'
file4='/g/data/xv83/dcfp/CAFE60v1/ocean_daily.zarr.zip'
file3='/g/data/xv83/rxm599/area.nc'

#dgrid=xr.open_dataset(file1)
docn = xr.open_zarr(file1,consolidated=True)

darea= xr.open_dataset(file3)
docnd = xr.open_zarr(file4,consolidated=True)

dbgc = xr.open_zarr(file2)

#darea= xr.open_dataset('/g/data/xv83/rxm599/grid_spec_dyn.nc')
darea



# + tags=[]
# create a thickness variable on the tracer depth grid
mdepth=docn.st_ocean.copy()
dbot=np.copy(docn.sw_ocean)
dthick=dbot*0
dthick[1:50]=dbot[1:50]-dbot[0:49]

dthick[0]=dbot[0]
#print(dthick,dbot)
mdepth=mdepth*0+dthick
mdepth

# + [markdown] jp-MarkdownHeadingCollapsed=true tags=[]
# # Define regions to extract 

# + tags=[]
ens=23
t1 = '2000-01-16'
t2 = '2018-12-31'
x1=-250 ; x2=-70
y1=-25.0 ; y2=25

tv=docn.ty_trans[:,ens,:,:].sel(time=slice(t1,t2),xt_ocean=slice(x1,x2),yu_ocean=slice(y1,y2))
tv_gm=docn.ty_trans_gm[:,ens,:,:].sel(time=slice(t1,t2),xt_ocean=slice(x1,x2),yu_ocean=slice(y1,y2))
tu=docn.tx_trans[:,ens,:,:].sel(time=slice(t1,t2),xu_ocean=slice(x1,x2),yt_ocean=slice(y1,y2))

tv_mean= tv.mean(axis=0)
tv_gm_mean=tv_gm.mean(axis=0)

tv_mean_sum1 = tv_mean[0:5,:,:].sum(axis=0)
tv_mean_sum2 = tv_mean[5:25,:,:].sum(axis=0)


#vt[0:10,:,:].sum(axis=(1,2)).plot()
# -


# # #### !pip install rechunker
# from  rechunker import rechunk
# max_mem = '100MB'
#
# target_store = 'v_rechunked.zarr'
# temp_store = 'rechunked-tmp.zarr'
#
# target_chunks = (1, 50, 109, 180)
#
# array_plan = rechunk(tv,target_chunks,max_mem, target_store, temp_store=temp_store)
# #array_plan = rechunk(docn.ty_trans[:,ens,:,:],target_chunks,max_mem, target_store, temp_store=temp_store)

# #### result = array_plan.execute()
# result.chunks

# +
#  write output
tv_mean_sum1.load()
tv_mean_sum2.load()
dir='/g/data/xv83/rxm599/tmp/'

tv_mean_sum1.to_netcdf(dir+'file1.nc')
tv_mean_sum2.to_netcdf(dir+'file2.nc')

tv_gm_mean.to_netcdf(dir+'file3.nc')
tv_gm.to_netcdf(dir+'tvgm_file.nc')

tv.to_netcdf(dir+'tv_file.nc')
# +

whos
# -


