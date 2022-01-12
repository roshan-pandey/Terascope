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


app_task = pd.merge(task_xy, app_check, on = ['jobId', 'taskId'], how='left')
app_task_gpu = pd.merge(app_task, gpu, on=['timestamp'])
del app_task_gpu['hostname_x']
app_task_gpu = app_task_gpu.rename(columns={'hostname_y': 'hostname'})
app_task_gpu.to_csv('./data/app_task_gpu.csv', index=False)


app_task_gpu = pd.read_csv("./data/app_task_gpu.csv")

app_task_gpu = app_task_gpu.loc[:,~app_task_gpu.columns.duplicated()]
app_task_gpu["time"] = app_task_gpu["timestamp"].apply(lambda x: parser.isoparse(x))
app_task_gpu['time'] = pd.Series([dt.time(d) for d in app_task_gpu['time']]) 

grouped = app_task_gpu.groupby(['eventName','eventType']).groups

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

render_df = pd.merge(render_start, render_stop, on='taskId')
save_conf_df = pd.merge(save_conf_start, save_conf_stop, on='taskId')
tiling_df = pd.merge(tiling_start, tiling_stop, on='taskId')
TotalRender_df = pd.merge(TotalRender_start, TotalRender_stop, on='taskId')
Uploading_df = pd.merge(Uploading_start, Uploading_stop, on='taskId')

render_df = render_df.drop(columns = [ 'x_x', 'y_x', 'level_x', 'gpuSerial_x', 'timestamp_x', 'timestamp_y', 'jobId_y', 'eventName_y', 'eventType_y', 'eventType_x', 'hostname_y', 'gpuUUID_x', 'powerDrawWatt_x', 'gpuTempC_x', 'gpuUtilPerc_x', 'gpuMemUtilPerc_x'])
save_conf_df = save_conf_df.drop(columns = ['x_x', 'y_x', 'level_x', 'gpuSerial_x', 'timestamp_x', 'timestamp_y', 'jobId_y', 'eventName_y', 'eventType_y', 'eventType_x', 'hostname_y', 'gpuUUID_x', 'powerDrawWatt_x', 'gpuTempC_x', 'gpuUtilPerc_x', 'gpuMemUtilPerc_x'])
tiling_df = tiling_df.drop(columns = ['x_x', 'y_x', 'level_x', 'gpuSerial_x', 'timestamp_x', 'timestamp_y', 'jobId_y', 'eventName_y', 'eventType_y', 'eventType_x', 'hostname_y', 'gpuUUID_x', 'powerDrawWatt_x', 'gpuTempC_x', 'gpuUtilPerc_x', 'gpuMemUtilPerc_x'])
TotalRender_df = TotalRender_df.drop(columns = ['x_x', 'y_x', 'level_x', 'gpuSerial_x', 'timestamp_x','timestamp_y', 'jobId_y', 'eventName_y', 'eventType_y', 'eventType_x', 'hostname_y',  'gpuUUID_x', 'powerDrawWatt_x', 'gpuTempC_x', 'gpuUtilPerc_x', 'gpuMemUtilPerc_x'])
Uploading_df = Uploading_df.drop(columns = ['x_x', 'y_x', 'level_x', 'gpuSerial_x', 'timestamp_x', 'timestamp_y', 'jobId_y', 'eventName_y', 'eventType_y', 'eventType_x', 'hostname_y', 'gpuUUID_x', 'powerDrawWatt_x', 'gpuTempC_x', 'gpuUtilPerc_x', 'gpuMemUtilPerc_x'])

render_df.columns = ['taskId', 'jobId', 'eventName', 'hostname', 'START', 'x', 'y', 'level', 'gpuSerial', 'gpuUUID', 'powerDrawWatt', 'gpuTempC', 'gpuUtilPerc', 'gpuMemUtilPerc', 'STOP']
save_conf_df.columns = ['taskId', 'jobId', 'eventName', 'hostname', 'START', 'x', 'y', 'level', 'gpuSerial', 'gpuUUID', 'powerDrawWatt', 'gpuTempC', 'gpuUtilPerc', 'gpuMemUtilPerc', 'STOP']
tiling_df.columns = ['taskId', 'jobId', 'eventName', 'hostname', 'START', 'x', 'y', 'level', 'gpuSerial', 'gpuUUID', 'powerDrawWatt', 'gpuTempC', 'gpuUtilPerc', 'gpuMemUtilPerc', 'STOP']
TotalRender_df.columns = ['taskId', 'jobId', 'eventName', 'hostname', 'START', 'x', 'y', 'level', 'gpuSerial', 'gpuUUID', 'powerDrawWatt', 'gpuTempC', 'gpuUtilPerc', 'gpuMemUtilPerc', 'STOP']
Uploading_df.columns = ['taskId', 'jobId', 'eventName', 'hostname', 'START', 'x', 'y', 'level', 'gpuSerial', 'gpuUUID', 'powerDrawWatt', 'gpuTempC', 'gpuUtilPerc', 'gpuMemUtilPerc', 'STOP']

render_df["duration"] = render_df.apply(lambda x: abs(dt.combine(date.today(), x['START']) - dt.combine(date.today(), x['STOP'])), axis=1)
save_conf_df["duration"] = save_conf_df.apply(lambda x: abs(dt.combine(date.today(), x['START']) - dt.combine(date.today(), x['STOP'])), axis=1)
tiling_df["duration"] = tiling_df.apply(lambda x: abs(dt.combine(date.today(), x['START']) - dt.combine(date.today(), x['STOP'])), axis=1)
TotalRender_df["duration"] = TotalRender_df.apply(lambda x: abs(dt.combine(date.today(), x['START']) - dt.combine(date.today(), x['STOP'])), axis=1)
Uploading_df["duration"] = Uploading_df.apply(lambda x: abs(dt.combine(date.today(), x['START']) - dt.combine(date.today(), x['STOP'])), axis=1)

full_df = pd.concat([render_df, save_conf_df, tiling_df, TotalRender_df, Uploading_df], axis=0)
full_df['duration'] = (full_df['duration'].dt.total_seconds())
full_df.to_csv("./data/merged_data.csv")
####################################################################################################################################
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


app_gpu_dt = pd.merge(app_check, gpu, on = ['hostname', 'timestamp'])
app_gpu_dt.to_csv('./data/app_gpu_with_timestamp.csv', index=False)

app_gpu_task = pd.merge(app_gpu_dt, task_xy, on = ['jobId', 'taskId'])
app_gpu_task.to_csv('./data/app_gpu_task.csv', index=False)





