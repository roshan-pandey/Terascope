import pandas as pd
import dask
import dask.dataframe as dd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
# import IPython
import os
from datetime import datetime as dt, timedelta, date
from tqdm import tqdm
from dateutil import parser

app_check = pd.read_csv("./data/application-checkpoints.csv") 
gpu = pd.read_csv("./data/gpu.csv")
task_xy = pd.read_csv("./data/task-x-y.csv")

app_check = app_check.loc[:,~app_check.columns.duplicated()]
app_check["time"] = app_check["timestamp"].apply(lambda x: parser.isoparse(x))
app_check['time'] = pd.Series([dt.time(d) for d in app_check['time']]) 

grouped = app_check.groupby(['eventName','eventType']).groups

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

render_start = app_check.iloc[rstrt_indx]
render_stop = app_check.iloc[rstop_indx]
save_conf_start = app_check.iloc[scstrt_indx]
save_conf_stop = app_check.iloc[scstop_indx]
tiling_start = app_check.iloc[tstrt_indx]
tiling_stop = app_check.iloc[tstop_indx]
TotalRender_start = app_check.iloc[trstrt_indx]
TotalRender_stop = app_check.iloc[trstop_indx]
Uploading_start = app_check.iloc[ustrt_indx]
Uploading_stop = app_check.iloc[ustop_indx]

render_df = pd.merge(render_start, render_stop, on='taskId')
save_conf_df = pd.merge(save_conf_start, save_conf_stop, on='taskId')
tiling_df = pd.merge(tiling_start, tiling_stop, on='taskId')
TotalRender_df = pd.merge(TotalRender_start, TotalRender_stop, on='taskId')
Uploading_df = pd.merge(Uploading_start, Uploading_stop, on='taskId')

render_df = render_df.drop(columns = ['timestamp_x', 'timestamp_y', 'hostname_y', 'jobId_y', 'eventType_x', 'eventName_y', 'eventType_y'])
save_conf_df = save_conf_df.drop(columns = ['timestamp_x', 'timestamp_y', 'hostname_y', 'jobId_y', 'eventType_x', 'eventName_y', 'eventType_y'])
tiling_df = tiling_df.drop(columns = ['timestamp_x', 'timestamp_y', 'hostname_y', 'jobId_y', 'eventType_x', 'eventName_y', 'eventType_y'])
TotalRender_df = TotalRender_df.drop(columns = ['timestamp_x', 'timestamp_y', 'hostname_y', 'jobId_y', 'eventType_x', 'eventName_y', 'eventType_y'])
Uploading_df = Uploading_df.drop(columns = ['timestamp_x', 'timestamp_y', 'hostname_y', 'jobId_y', 'eventType_x', 'eventName_y', 'eventType_y'])

render_df.columns = ['hostname', 'eventName', 'jobId', 'taskId', 'START', 'STOP']
save_conf_df.columns = ['hostname', 'eventName', 'jobId', 'taskId', 'START', 'STOP']
tiling_df.columns = ['hostname', 'eventName', 'jobId', 'taskId', 'START', 'STOP']
TotalRender_df.columns = ['hostname', 'eventName', 'jobId', 'taskId', 'START', 'STOP']
Uploading_df.columns = ['hostname', 'eventName', 'jobId', 'taskId', 'START', 'STOP']

render_df["duration"] = render_df.apply(lambda x: abs(dt.combine(date.today(), x['START']) - dt.combine(date.today(), x['STOP'])), axis=1)
save_conf_df["duration"] = save_conf_df.apply(lambda x: abs(dt.combine(date.today(), x['START']) - dt.combine(date.today(), x['STOP'])), axis=1)
tiling_df["duration"] = tiling_df.apply(lambda x: abs(dt.combine(date.today(), x['START']) - dt.combine(date.today(), x['STOP'])), axis=1)
TotalRender_df["duration"] = TotalRender_df.apply(lambda x: abs(dt.combine(date.today(), x['START']) - dt.combine(date.today(), x['STOP'])), axis=1)
Uploading_df["duration"] = Uploading_df.apply(lambda x: abs(dt.combine(date.today(), x['START']) - dt.combine(date.today(), x['STOP'])), axis=1)

full_df = pd.concat([render_df, save_conf_df, tiling_df, TotalRender_df, Uploading_df], axis=0)
full_df['duration'] = (full_df['duration'].dt.total_seconds())
full_df.to_csv("./data/processed_app_conf.csv")

render_time = render_df['duration'].sum()
save_conf_time = save_conf_df['duration'].sum()
tiling_time = tiling_df['duration'].sum()
TotalRender_time = TotalRender_df['duration'].sum()
Uploading_time = Uploading_df['duration'].sum()

eventName = app_check['eventName'].unique()
total_duration = [tiling_time, save_conf_time, render_time, TotalRender_time, Uploading_time]
duration_df = pd.DataFrame({'eventName': eventName, 'totalDuration': total_duration})
duration_df['totalDuration'] = (duration_df['totalDuration'].dt.total_seconds()/3600).round(3)

duration_df.to_csv("./data/event-total-duration.csv")

app_check_dd = dd.read_csv('./data/processed_app_conf.csv')
gpu_dd = dd.read_csv('./data/gpu.csv')
app_gpu = dd.merge(app_check_dd, gpu_dd, on='hostname')
app_gpu.to_csv("./data/app_gpu.csv", index=False, single_file=True)

app_gpu = dd.read_csv("./data/app_gpu.csv")
task_xy_dd = dd.read_csv("./data/task-x-y.csv")
app_gpu_task = dd.merge(app_gpu, task_xy_dd, on='taskId')
del app_gpu_task['jobId_x']
app_gpu_task = app_gpu_task.rename(columns={'jobId_y': 'jobId'})

data = app_gpu_task[['x', 'y', 'powerDrawWatt']]
data = data.groupby(['x', 'y']).mean()
data = data.reset_index()
data_pd = data.compute()
pivot_table = data_pd.pivot('x', 'y','powerDrawWatt')

# data = app_gpu_task[['x', 'y', 'powerDrawWatt']]
# data_pvt = data.pivot_table("x", "y", "powerDrawWatt")
ax = sns.heatmap(pivot_table)
ax.invert_yaxis()
plt.show()
app_gpu_task.to_csv('./data/app_gpu_task.csv', index=False, single_file=True)