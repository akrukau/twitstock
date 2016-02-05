import fnmatch
import subprocess
import os

merged_file = "./merged-2015-sample.json" 
subprocess.call(["touch", merged_file])
for root, dirnames, filenames in os.walk('./2015'):
    for filename in fnmatch.filter(filenames, '00.json'):
        path = os.path.join(root, filename)
        os.system("cat " + path + " >> " + merged_file)

