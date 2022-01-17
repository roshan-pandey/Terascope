########################################################################################################
#                                   F I L E   D E S C R I P T I O N                                    #
########################################################################################################
#                                                                                                      #
# This file contains the code related to Data Munging: transformations like, joining different tables, #
# merging tables row wise and column wise, column and row renaming, column and row deletion, etc.      #                                                                           #
########################################################################################################

# Importing relevant packages...
import pandas as pd
import numpy as np
from datetime import datetime as dt, date
from dateutil import parser


#Loading data...
app_check = pd.read_csv("./data/application-checkpoints.csv") 
gpu = pd.read_csv("./data/gpu.csv")
task_xy = pd.read_csv("./data/task-x-y.csv")


app_task = pd.merge(task_xy, app_check, on = ['jobId', 'taskId'], how='left') # left joining task-x-y and application-checkpoint data on jobId and taskId...
app_task_gpu = pd.merge(app_task, gpu, on=['timestamp']) # inner joining gpu data with merged data from previous step on timestamp...
del app_task_gpu['hostname_x'] # Deleting duplicate column 'hostname_x'
app_task_gpu = app_task_gpu.rename(columns={'hostname_y': 'hostname'}) # Renaming 'hostname_y' to 'hostname'


app_task_gpu = app_task_gpu.loc[:,~app_task_gpu.columns.duplicated()] # deleting duplicate rows...
app_task_gpu["time"] = app_task_gpu["timestamp"].apply(lambda x: parser.isoparse(x)) # extracting only time part of the timestamp column and storing it in "time" column...
app_task_gpu['time'] = pd.Series([dt.time(d) for d in app_task_gpu['time']]) # changing data type of the "time" column to time...

grouped = app_task_gpu.groupby(['eventName','eventType']).groups # grouping on eventName and eventType...


# getting index of each event's start and stop rows and storing them in seperate variables...
rstrt_indx = np.array(np.array(grouped[('Render', 'START')]))
rstop_indx = np.array(np.array(grouped[('Render', 'STOP')]))
scstrt_indx = np.array(np.array(grouped[('Saving Config', 'START')]))
scstop_indx = np.array(np.array(grouped[('Saving Config', 'STOP')]))
tstrt_indx = np.array(np.array(grouped[('Tiling', 'START')]))
tstop_indx = np.array(np.array(grouped[('Tiling', 'STOP')]))
trstrt_indx = np.array(np.array(grouped[('TotalRender', 'START')]))
trstop_indx = np.array(np.array(grouped[('TotalRender', 'STOP')]))
ustrt_indx = np.array(np.array(grouped[('Uploading', 'START')]))
ustop_indx = np.array(np.array(grouped[('Uploading', 'STOP')]))


# selecting rows based on the index found out in the previous step...
render_start = app_task_gpu.iloc[rstrt_indx]
render_stop = app_task_gpu.iloc[rstop_indx]
save_conf_start = app_task_gpu.iloc[scstrt_indx]
save_conf_stop = app_task_gpu.iloc[scstop_indx]
tiling_start = app_task_gpu.iloc[tstrt_indx]
tiling_stop = app_task_gpu.iloc[tstop_indx]
TotalRender_start = app_task_gpu.iloc[trstrt_indx]
TotalRender_stop = app_task_gpu.iloc[trstop_indx]
Uploading_start = app_task_gpu.iloc[ustrt_indx]
Uploading_stop = app_task_gpu.iloc[ustop_indx]


# inner joining start and stop dataframes of respective events on taskId...
render_df = pd.merge(render_start, render_stop, on='taskId')
save_conf_df = pd.merge(save_conf_start, save_conf_stop, on='taskId')
tiling_df = pd.merge(tiling_start, tiling_stop, on='taskId')
TotalRender_df = pd.merge(TotalRender_start, TotalRender_stop, on='taskId')
Uploading_df = pd.merge(Uploading_start, Uploading_stop, on='taskId')


# dropping duplicate columns...
render_df = render_df.drop(columns = [ 'x_x', 'y_x', 'level_x', 'gpuSerial_x', 'timestamp_x', 'timestamp_y', 'jobId_y', 'eventName_y', 'eventType_y', 'eventType_x', 'hostname_y', 'gpuUUID_x', 'powerDrawWatt_x', 'gpuTempC_x', 'gpuUtilPerc_x', 'gpuMemUtilPerc_x'])
save_conf_df = save_conf_df.drop(columns = ['x_x', 'y_x', 'level_x', 'gpuSerial_x', 'timestamp_x', 'timestamp_y', 'jobId_y', 'eventName_y', 'eventType_y', 'eventType_x', 'hostname_y', 'gpuUUID_x', 'powerDrawWatt_x', 'gpuTempC_x', 'gpuUtilPerc_x', 'gpuMemUtilPerc_x'])
tiling_df = tiling_df.drop(columns = ['x_x', 'y_x', 'level_x', 'gpuSerial_x', 'timestamp_x', 'timestamp_y', 'jobId_y', 'eventName_y', 'eventType_y', 'eventType_x', 'hostname_y', 'gpuUUID_x', 'powerDrawWatt_x', 'gpuTempC_x', 'gpuUtilPerc_x', 'gpuMemUtilPerc_x'])
TotalRender_df = TotalRender_df.drop(columns = ['x_x', 'y_x', 'level_x', 'gpuSerial_x', 'timestamp_x','timestamp_y', 'jobId_y', 'eventName_y', 'eventType_y', 'eventType_x', 'hostname_y',  'gpuUUID_x', 'powerDrawWatt_x', 'gpuTempC_x', 'gpuUtilPerc_x', 'gpuMemUtilPerc_x'])
Uploading_df = Uploading_df.drop(columns = ['x_x', 'y_x', 'level_x', 'gpuSerial_x', 'timestamp_x', 'timestamp_y', 'jobId_y', 'eventName_y', 'eventType_y', 'eventType_x', 'hostname_y', 'gpuUUID_x', 'powerDrawWatt_x', 'gpuTempC_x', 'gpuUtilPerc_x', 'gpuMemUtilPerc_x'])


# renaming the coulmns...
render_df.columns = ['taskId', 'jobId', 'eventName', 'hostname', 'START', 'x', 'y', 'level', 'gpuSerial', 'gpuUUID', 'powerDrawWatt', 'gpuTempC', 'gpuUtilPerc', 'gpuMemUtilPerc', 'STOP']
save_conf_df.columns = ['taskId', 'jobId', 'eventName', 'hostname', 'START', 'x', 'y', 'level', 'gpuSerial', 'gpuUUID', 'powerDrawWatt', 'gpuTempC', 'gpuUtilPerc', 'gpuMemUtilPerc', 'STOP']
tiling_df.columns = ['taskId', 'jobId', 'eventName', 'hostname', 'START', 'x', 'y', 'level', 'gpuSerial', 'gpuUUID', 'powerDrawWatt', 'gpuTempC', 'gpuUtilPerc', 'gpuMemUtilPerc', 'STOP']
TotalRender_df.columns = ['taskId', 'jobId', 'eventName', 'hostname', 'START', 'x', 'y', 'level', 'gpuSerial', 'gpuUUID', 'powerDrawWatt', 'gpuTempC', 'gpuUtilPerc', 'gpuMemUtilPerc', 'STOP']
Uploading_df.columns = ['taskId', 'jobId', 'eventName', 'hostname', 'START', 'x', 'y', 'level', 'gpuSerial', 'gpuUUID', 'powerDrawWatt', 'gpuTempC', 'gpuUtilPerc', 'gpuMemUtilPerc', 'STOP']


# Calculating the duration of each event by subtracting the start and stop columns...
render_df["duration"] = render_df.apply(lambda x: abs(dt.combine(date.today(), x['START']) - dt.combine(date.today(), x['STOP'])), axis=1)
save_conf_df["duration"] = save_conf_df.apply(lambda x: abs(dt.combine(date.today(), x['START']) - dt.combine(date.today(), x['STOP'])), axis=1)
tiling_df["duration"] = tiling_df.apply(lambda x: abs(dt.combine(date.today(), x['START']) - dt.combine(date.today(), x['STOP'])), axis=1)
TotalRender_df["duration"] = TotalRender_df.apply(lambda x: abs(dt.combine(date.today(), x['START']) - dt.combine(date.today(), x['STOP'])), axis=1)
Uploading_df["duration"] = Uploading_df.apply(lambda x: abs(dt.combine(date.today(), x['START']) - dt.combine(date.today(), x['STOP'])), axis=1)


# calculating the total time taken by each event during the period of time data was collected by adding time/duration calculated in the previous step...
render_time = render_df['duration'].sum()
save_conf_time = save_conf_df['duration'].sum()
tiling_time = tiling_df['duration'].sum()
TotalRender_time = TotalRender_df['duration'].sum()
Uploading_time = Uploading_df['duration'].sum()


eventName = app_check['eventName'].unique() # taking out unique eventNames... (5 events: Render, Saving Config, Tiling, TotalRender, Uploading)
total_duration = [tiling_time, save_conf_time, render_time, TotalRender_time, Uploading_time] # saving all the total times in one list...
duration_df = pd.DataFrame({'eventName': eventName, 'totalDuration': total_duration}) # dreating a dataframe of eventNames and their total times...
duration_df['totalDuration'] = (duration_df['totalDuration'].dt.total_seconds()/3600).round(3) # converting the total time taken to seconds... 

duration_df.to_csv("./data/event-total-duration.csv") # storing it in a csv file...

full_df = pd.concat([render_df, save_conf_df, tiling_df, TotalRender_df, Uploading_df], axis=0) # verticallling stacking all the event's datafraeme into one...
full_df['duration'] = (full_df['duration'].dt.total_seconds()) # converting the time to seconds...
full_df.to_csv("./data/merged_data.csv") # storing it as a csv...