import os 

# Use Sparkcontext object's configuration to get the locations - Source/Destination
# I was trying to copy files from various subdirectories on HDFS into One HDFS directory

hadoop = sc._jvm.org.apache.hadoop

fs = hadoop.fs.FileSystem
conf = hadoop.conf.Configuration() 
dirname = 'Path-To-Directory'
path = hadoop.fs.Path(dirname)

for f in fs.get(conf).listStatus(path):
    print f.getPath().getName()

# Looking for pattern 201 in the names of subdirectories
fdir = [ dirname+ f.getPath().getName() for f in fs.get(conf).listStatus(path) if '201' in f.getPath().getName()]
fnames = []
for i in fdir:
    p1 = hadoop.fs.Path(i)
    for ff in fs.get(conf).listStatus(p1):
        if 'Pattern-To-Check-In-Filename' in ff.getPath().getName():
            fnames.append(i + '/' + ff.getPath().getName())
            os.system('hadoop fs -cp ' + i + '/' + ff.getPath().getName() + ' Destination-Location')
