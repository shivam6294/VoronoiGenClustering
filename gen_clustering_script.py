__author__ = 'admin'

import sys
from configobj import *
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from IPython.display import Markdown, display
from sklearn import metrics
from sklearn.cluster import KMeans
from time import time
import subprocess

configFile = open(sys.argv[2])
appConfig = ConfigObj(configFile)

# Pull Data from Hadoop and write to foler hadoop_temp_data
machine_id = {}
machine_id['part_0'] = 0
machine_id['part_1'] = 1
machine_id['part_2'] = 2
machine_id['part_3'] = 3

BASHCMD_HadoopToLocalSystem = 'hadoop fs -get' + appConfig['hadoop_input_dir'] + "/%s " + \
    appConfig['temp_data_location'] + '/%s'

for key in machine_id.keys():
    current_bash_command = BASHCMD_HadoopToLocalSystem % (key,key)
    process = subprocess.Popen(current_bash_command.split(), stdout=subprocess.PIPE)
    output = process.communicate()[0]

print (appConfig)
print("Done")
# #Please input the location of the coordinate file, edge weight file and the output filename"
# file_identifier="FLA"
#
# # The geofile has the following schema:
# # ___________________________________________________
# # | entity type | vertex id | latitude | longitude |
# geofile="C:\\Users\\admin\\Downloads\\WinPython-64bit-2.7.10.1\\\
# notebooks\\USA_ROAD_NETWORK_DATASETS\\USA-road-d."+file_identifier+".co"
#
# # The distfile has the following schema:
# # __