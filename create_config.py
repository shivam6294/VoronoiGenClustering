from configobj import *

config = ConfigObj()

config['file_identifier'] = "FLA"




config['geofile'] = "C:\\Users\\admin\\Downloads\\WinPython-64bit-2.7.10.1\\notebooks\\" + \
"USA_ROAD_NETWORK_DATASETS\\USA-road-d."+config['file_identifier']+".co"

config['list_of_generators'] = [265, 336, 724, 962, 970, 971, 972, 1023, 1028, 1282, 1282, 1785, 1785, 1811, 4852, 4943, 4949]

config['hadoop_input_dir']

config['temp_data_location'] = '/root/data/Temp_Hadoop_Directory'

config.filename = config['file_identifier'] + ".conf"

config.write()
