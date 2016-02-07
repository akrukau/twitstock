import fnmatch
import subprocess
import os

for root, dirnames, filenames in os.walk('./2015'):
    for filename in fnmatch.filter(filenames, '*.json'):
        path = os.path.join(root, filename)
        print "bzip2 -z",path
        subprocess.call(["bzip2", "-z",  path])
