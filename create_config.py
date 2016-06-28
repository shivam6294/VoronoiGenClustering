from configobj import *

config = ConfigObj()
config['num_cores_on_machine'] = 4

config['file_identifier'] = "FLA"

config['geofile'] = "/root/data/GeoFiles/USA-road-d."+config['file_identifier']+".co"

config['list_of_generators'] = [265, 336, 724, 962, 970, 971, 972, 1023, 1028, 1282, 1282, 1785, 1785, 1811, 4852, 4943, 4949]

config['hadoop_input_dir'] = "/vor/usanewyork/"

config['local_location'] = '/root/data/Temp_Hadoop_Directory/'

config.filename = config['file_identifier'] + ".conf"

config.write()
