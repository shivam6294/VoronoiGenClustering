__author__ = 'shivam6294'

import sys
from configobj import *
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from sklearn import metrics
from sklearn.cluster import KMeans
from time import time
import subprocess

if len(sys.argv) <= 2 or len(sys.argv) > 3:
    print("Correct usage: python gen_clustering_script.py config_file.conf")
    print("Fatal Error. Incorrect args.")
    exit()
configFile = open(sys.argv[2])
appConfig = ConfigObj(configFile)


def execBashCommand(cmd):
    process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
    output = process.communicate()[0]

# Strict checking of config params
def sanityCheck(appConfig):
    if type(appConfig['num_cores_on_machine']) != str or \
        type(appConfig['geofile']) != str or  \
        type(appConfig['list_of_generators']) != list or \
        type(appConfig['hadoop_input_dir']) != str or \
        appConfig['hadoop_input_dir'].count('/') != 3 or \
        type(appConfig['local_location']) != str or \
        appConfig['local_location'].count('/') != 4 or \
        appConfig['geofile'].count('/') != 4:
        print(type(appConfig['num_cores_on_machine']))
        print(type(appConfig['geofile']))
        print(type(appConfig['list_of_generators']))
        print(type(appConfig['hadoop_input_dir']))
        print(appConfig['hadoop_input_dir'].count('/'))
        print(type(appConfig['local_location']))
        print("Please check config file. Fatal Error. Program exiting.")
        exit()

sanityCheck(appConfig)
print(appConfig['num_cores_on_machine'])
num_cores_on_machine = int(appConfig['num_cores_on_machine'])

# Pull Data from Hadoop and write to folder hadoop_temp_data
machine_id = {}
machine_id['part_0'] = 0
machine_id['part_1'] = 1
machine_id['part_2'] = 2
machine_id['part_3'] = 3

# Clean up local input location
BASHCMD_CleanLocalDir = "rm -rf "+appConfig['local_location']+"*"
execBashCommand(BASHCMD_CleanLocalDir)


BASHCMD_HadoopToLocalSystem = 'hadoop fs -get' + appConfig['hadoop_input_dir'] + "%s " + \
  appConfig['local_location'] + '%s'


for key in machine_id.keys():
   current_bash_command = BASHCMD_HadoopToLocalSystem % (key,key)
   execBashCommand(current_bash_command)

# Loading the data from the geolocation (lat,long) into a python dictionary (Hash-Map) with the graph vertex as key
geofile = appConfig['geofile']

count = 0
LOC_DICT = {}
with open(geofile) as f:
    for line in f:
        arr = line.split(' ')
        if arr[0] == 'v':
                key = arr[1].strip()
                val_list = []
                val_list.append(float(arr[2].strip())*10**-6)
                val_list.append(float(arr[3].strip())*10**-6)
                LOC_DICT[key] = val_list


## Importing the output files from all processes
files_dict = {}
vertex_to_block_id = {}
for i in range(0,num_cores_on_machine):
    filename_string = "part_"+str(i)
    current_file = appConfig['local_location'] + filename_string
    vertices_on_machine = {}
    with open(current_file) as f:
        for line in f:
            vertex = ((line.split(sep='\t')[0]).split(sep=' ')[0])
            vertices_on_machine[vertex] = line.split(sep='\t')[1]
            vertex_to_block_id[vertex] = ((line.split(sep='\t')[0]).split(sep=' ')[1])
        files_dict[filename_string] = vertices_on_machine

print('There are '+ str(len(vertices_on_machine)) + ' vertices on hadoop output file- '+filename_string)

# Creating new dataframe
dataframes = {}
for key in machine_id.keys():
    dataframes[key] = pd.DataFrame(columns=['Vertex','adjacency_list'],data=list(files_dict[key].items()))
    dataframes[key] = dataframes[key].set_index('Vertex')
    temp_df = pd.DataFrame.from_dict(vertex_to_block_id,orient ='index')
    temp_df.index.rename('Vertex',inplace=True)
    temp_df.columns = ['block_id']
    dataframes[key]['from_file'] = key
    dataframes[key]['machine_id'] = machine_id[key]
    dataframes[key] = pd.merge(dataframes[key],temp_df,right_index=True,left_index=True)

geo_df = pd.DataFrame.from_dict(LOC_DICT,orient='index')
geo_df.columns= ['lat','long']

for key in machine_id.keys():
    dataframes[key] = pd.merge(dataframes[key],geo_df,how='left',left_index=True,right_index=True)

## Now, we have the data that we need for clustering
## Let's go over each machine dataframe, and check if each machine is even assigned more than 4 generators
generators_in_machine = {}
for key in machine_id.keys():
    df_set_of_vertices_in_machine = set(list(map(str, dataframes[key].index)))
    set_of_generators = set(appConfig['list_of_generators'])
    generators_in_machine[key] = df_set_of_vertices_in_machine.intersection(set_of_generators)
print(generators_in_machine)


### Let's FILTER the vertices based on if they are generators
# for each dataframe:
#     check if there are any generators present

def doManualAssignment(dataframe, value):
    ctr = 0
    for x in value:
        dataframe = dataframe.set_value(x,'ClusterID', ctr)
        ctr = ctr + 1
    return dataframe

kmeans_estimators = {}
def doClustering(dataframe, key, value):
    kmeans_estimators[key] = KMeans(init='k-means++', n_clusters=4, n_init=10)
    dataframe_temp = dataframe.copy()
    dataframe_temp = dataframe_temp.reset_index()
    dataframe_temp = dataframe_temp[dataframe_temp['Vertex'].isin(appConfig['list_of_generators'])]
    # CREATE TEMP DATAFRAME WITH GENERATORS ONLY
    dataframe_temp['ClusterID'] = get_labels_k_means(kmeans_estimators[key],
                                         data=dataframe_temp[['lat','long']])
    dataframe = dataframe.drop(['ClusterID'],axis=1)
    dataframe_temp = dataframe_temp.set_index('Vertex')
    merged_dataframe = pd.merge(dataframe,dataframe_temp[['ClusterID']],how='left',left_index = True, right_index=True)
    merged_dataframe['ClusterID'].fillna(-999,inplace=True)
    merged_dataframe['ClusterID'] = merged_dataframe['ClusterID'].astype(np.int64)
    return merged_dataframe

def get_labels_k_means(estimator, data):
    t0 = time()
    return estimator.fit_predict(data)

for key,value in generators_in_machine.items():
    print("Number of generators in " + key +" : " + str(len(value)))
    dataframes[key]['ClusterID'] = -999 # Setting the default cluster id
    if len(value) == 0 :
        continue
    elif len(value) <=4 :
        dataframes[key] = doManualAssignment(dataframes[key], value)
    else : # PERFORM Clustering
        dataframes[key] = doClustering (dataframes[key], key , value)
        print(dataframes[key] )

def printProgress (iteration, total, prefix = '', suffix = '', decimals = 2, barLength = 100):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : number of decimals in percent complete (Int)
        barLength   - Optional  : character length of bar (Int)
    """
    filledLength    = int(round(barLength * iteration / float(total)))
    percents        = round(100.00 * (iteration / float(total)), decimals)
    bar             = '#' * filledLength + '-' * (barLength - filledLength)
    sys.stdout.write('%s [%s] %s%s %s\r' % (prefix, bar, percents, '%', suffix)),
    sys.stdout.flush()
    if iteration == total:
        print("\n")

i     = 0
l     = len(vertices_on_machine)*num_cores_on_machine

if appConfig['local_location'] == appConfig['output_location']:
    for key in machine_id.keys():
       execBashCommand("rm -rf " + appConfig['local_location']+key)
       print("Deleted file - " + key)

    execBashCommand("rm -rf " + appConfig['local_location']+key)

# HEAVY DISK I/O
printProgress(i, l, prefix = 'Progress:', suffix = 'Complete', barLength = 50)
big_line_buffer = ""
for key in machine_id.keys():
    for key,value in dataframes[key].iterrows():
        i += 1
        line_str=""
        line_str = line_str + str(key) + " " +str(value['block_id'])+ " "  + str(value['machine_id'])+ " "  + str(value['ClusterID'])+ "\t" + str(value['adjacency_list'])
        big_line_buffer = big_line_buffer + '\n' + line_str
        if (i % 10000) == 0:
            with open(appConfig['output_location']+value['from_file'], 'a') as f:
                print(big_line_buffer, file=f)
                big_line_buffer = ""
            f.close()
            printProgress(i, l, prefix = 'Progress:', suffix = 'Complete', barLength = 50)

if appConfig['hadoop_input_dir'] != '':
   BASHCMD_delete_hadoop_directory = "hadoop fs -rm "+ appConfig['hadoop_input_dir']+ '*'

BASHCMD_LocalSystemToHadoop = 'hadoop fs -put' +appConfig['local_location'] + "%s " + \
   appConfig['hadoop_input_dir'] + '%s'

for key in machine_id.keys():
   current_bash_command = BASHCMD_HadoopToLocalSystem % (key,key)
   execBashCommand(current_bash_command)

print("...............Done..............")
