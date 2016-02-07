import fnmatch
import subprocess
import os

for root, dirnames, filenames in os.walk('./2015'):
    for filename in fnmatch.filter(filenames, '*.json.bz2'):
        path = os.path.join(root, filename)
        print "Appending",path
        subprocess.call(["hdfs", "dfs", "-appendToFile", path, "/tweets/full-tweets-2015.bz2"])
        subprocess.call(["rm", "-f", path])
