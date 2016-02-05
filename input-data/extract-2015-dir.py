import fnmatch
import subprocess
import os

matches = []
for root, dirnames, filenames in os.walk('./2015'):
    for filename in fnmatch.filter(filenames, '00.json.bz2'):
        path = os.path.join(root, filename)
        print "bzip2 -d",path
        subprocess.call(["bzip2", "-d",  path])
        matches.append(path)
