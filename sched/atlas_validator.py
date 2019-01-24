#!/usr/bin/env python
#
# Validation script for ATLAS WU
# The script_validator should be configured to all this script like this:
# script_validator --app ATLAS --init_script "atlas_validator.py files runtime" --compare_script "true"
#
# Arguments: <output files> <CPU time>
#
import commands
import xml.dom.minidom
import os, sys, tarfile, re

# Check for one or two output files
if len(sys.argv) not in [3, 4]:
    sys.exit(-1)
ResultFile = sys.argv[1]
if len(sys.argv) == 4:
    print "Separate HITS file"
    HitsFile = sys.argv[2]
else:
    print "HITS in tarball"
    HitsFile = None
boinccputime = float(sys.argv[-1])
print "CPU time from BOINC: %d" % boinccputime

jid = os.path.basename(ResultFile).split("_")[0]
Edir="/tmp/%s"%jid
if not os.path.exists(Edir):
    os.mkdir(Edir)
t1 = tarfile.open(sys.argv[1])
cputime = 0
walltime = 0
try:
    diag = t1.getmember("%s.diag" % jid)
    if diag:
        t1.extract(diag, path=Edir)
        with open('%s/%s.diag' % (Edir, jid)) as f:
            data = f.read()
            print data
        usertime = re.search('UserTime=(\d*)', data)
        processors = re.search('Processors=(\d*)', data)
        if processors:
            processors = int(processors.group(1))
        else:
            processors = 1
        if usertime:
            cputime = int(usertime.group(1)) * processors
            print 'CPU time measured by ARC %d (%d x %d)' % (cputime, cputime/processors, processors)
        wallclock = re.search('WallTime=(\d*)', data)
        if wallclock:
            walltime = int(wallclock.group(1))
            print 'Wall time %d' % walltime
except Exception, e:
    print str(e)
## Pass until we debug cause of missing file
s = None
try:
    s=t1.getmember("./jobSmallFiles.tgz")
except:
    #sys.exit(0)
    pass

if s:
    t1.extract(s, path=Edir)
else:
    print 'Failed to extract from jobSmallFiles.tgz'
    if boinccputime > 900 or walltime > 3600:
        print 'CPU or walltime is large enough, job is validated'
        sys.exit(0)
    sys.exit(1)
if os.path.exists("%s/jobSmallFiles.tgz"%Edir):
    t2 = tarfile.open("%s/jobSmallFiles.tgz"%Edir, "r:gz")
    m = t2.getmember("metadata-surl.xml")
    if m:
        t2.extract(m, path=Edir)
    else:
        print "metadata-surl.xml does not exist!"
        sys.exit(1)
else:
    print "jobSmallFiles.tgz does not exisit!"
    sys.exit(1)

if not os.path.exists("%s/metadata-surl.xml"%Edir):
    sys.exit(1)
LOG=0
ROOT=0
MetaFile = "%s/metadata-surl.xml"%Edir
outputxml = xml.dom.minidom.parse(MetaFile)
files = outputxml.getElementsByTagName("POOLFILECATALOG")[0].getElementsByTagName("File")
for f in files:
    try:
        for m in  f.getElementsByTagName ("metadata"):
            v = m.getAttribute("att_value")
            if m.getAttribute("att_name") == "surl":
                surl = v
                print "surl=%s"%surl
                sfile = surl.split("/")[-1]
                if sfile.find(".job.log.tgz") >= 0:
                    try:
                        ff = t1.getmember("./%s"%sfile)
                        print "./%s file exist!" %sfile
                        LOG = 1
                    except:
                        try:
                            ff = t1.getmember(sfile)
                            LOG = 1
                            print "%s file exist!" %sfile
                        except:
                            print "%s is in metadata-surl.xml, but not in the tar.gz file"%sfile

                elif sfile.find(".root.") >=0:
                    # Separate HITS file
                    if HitsFile:
                        if os.path.exists(HitsFile):
                            ROOT = 1
                    # HITS inside tar
                    else:
                        try:
                            ff = t1.getmember("./%s"%sfile)
                            print "./%s file exist!" %sfile
                            ROOT = 1
                        except:
                            try:
                                ff = t1.getmember(sfile)
                                ROOT = 1
                                print "%s file exist!" %sfile
                            except:
                                print "%s is in metadata-surl.xml, but not in the tar.gz file"%sfile

    except Exception, x:
        print "failed to parse the xml file"
#os.rmdir(Edir)
t1.close()
t2.close()

commands.getoutput("rm -fr %s"%Edir)
# If at least the log is there then better to validate and pass the error back to panda
# Re-introduce the check because it gives too much credit to badly-configured hosts
if LOG == 1 and ROOT == 1:
    print "All expected output files present"
    sys.exit(0)

# If CPU time is over 20 mins or walltime over 1hr let the job go through even if it failed
if boinccputime > 1500 or walltime > 3600:
    print "CPU time or walltime is long enough to pass validation"
    sys.exit(0)

print 'An output file is missing, and cputime<15minutes , validation failed!'
sys.exit(1)

