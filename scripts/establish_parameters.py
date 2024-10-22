#!/usr/bin/env python
# coding: utf-8
import pandas as pd
import numpy as np
import os
pd.options.mode.chained_assignment = None  # default='warn'

# Our boundaries will be:
# - xlim = ([-125, -66])  #Longitude
# - ylim = ([24, 50])  #Latitude
# 
# And our step size will be:
# - Easterly: 9.291531215312152e-06
# - Northerly: 8.993999191439993e-06
# 
# Noting that this easterly movement is appropriate to 1 mile at the top of our map, and will outperform at the bottom



# # Establish dummy files
# ## Chunk 1: x coordinates
# 
# We'll want to divide the latitude range into 10 chunks, and likewise for the longitude range. Thus, we will have 10x10 output files.So: 
# - Divide len(xx) by 10. This is the size of each output file.
# - Assign the first output file an integer, 0. Repeat this integer until we have enough x values, and then increase the integer by 1, and do the same for the next batch of x values, until all x values have been assigned one of ten integers, flagging the file they'll output to.

# In[13]:


xx = np.arange(start=-125, stop=-66, step=9.291531215312152e-06)
yy = np.arange(start=24, stop=50, step=8.993999191439993e-06)
len_xx = len(xx)
len_yy = len(yy)

num_records_per_chunk_x = np.ceil(len_xx / 200) 
num_records_per_chunk_y = np.ceil(len_yy / 200)


# Chunk 1: x coordinates
chunk_array_x = np.zeros(len_xx)
chunk_num = 0
counter = 0
for i in np.arange(len_xx):
    chunk_array_x[i] = int(chunk_num)
    counter = int(counter + 1)
    is_auspicious = counter % int(num_records_per_chunk_x) == 0
    if is_auspicious:
        chunk_num = int(chunk_num + 1)

# Chunk 2: y coordinates

chunk_array_y = np.zeros(len_yy)
chunk_num = 0
counter = 0
for i in np.arange(len_yy):
    chunk_array_y[i] = int(chunk_num)
    counter = int(counter + 1)
    is_auspicious = counter % int(num_records_per_chunk_y) == 0
    if is_auspicious:
        chunk_num = int(chunk_num + 1)


my_x_chunk = 0
FullX = pd.DataFrame({'x':xx, 'chunk_x':chunk_array_x, 'key':1})
SubsetX = FullX.loc[FullX['chunk_x'] == my_x_chunk]

my_y_chunk = 0
FullY = pd.DataFrame({'y':yy, 'chunk_y':chunk_array_y, 'key':1})
SubsetY = FullY.loc[FullY['chunk_y'] == my_y_chunk]

dir = '/Volumes/Extreme SSD/largest_plots/clean_data'
fn = str(my_x_chunk) + '_' + str(my_y_chunk)
fn_suffix = '.parquet'
full_fn = os.path.join(dir, fn + fn_suffix)


SubsetX.merge(SubsetY, on='key', how='outer').to_parquet(full_fn)
