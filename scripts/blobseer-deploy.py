#!/usr/bin/env python

# This script launches BlobSeer on all configured machines
import threading
import getopt
import sys
import os
import subprocess

# What ssh like command are we using? (maybe oarsh :) )
SSH="ssh"
SCP="scp"
TMP_CFG_FILE="./blobseer-deploy.cfg"
REMOTE_CFG_FILE="/tmp/blobseer.cfg"

def usage():
    print "%s [-v, --vmgr <hostname>] [-m, --pmgr <hostname>] [-d, --dht <dht.txt>] [-p, --providers <providers.txt>] [ [-l, --launch] | [-k, --kill] | [-s, --status] ]\n" % sys.argv[0]

def read_hosts(file_name):
    f = open(file_name, "r")
    hosts = map(lambda x: x.strip(), filter(lambda x: x != "\n", f.readlines()))
    f.close()
    return hosts

sys_vars = {}

def get_env(name):
    if name in sys_vars:
        return sys_vars[name]
    try:
        sys_vars[name] = os.environ[name]
    except KeyError:
        print "WARNING: environment variable " + name + " is not set"
        sys_vars[name] = "."
    return sys_vars[name]

def gen_launch(path, process):
    out = "/tmp/blobseer/" + process
    return "\"rm -rf " + out + "; " + \
        "mkdir -p " + out + "; " + \
        "env CLASSPATH=" + get_env("CLASSPATH") + " " + \
        "LD_LIBRARY_PATH=" + get_env("LD_LIBRARY_PATH") + " " + \
        get_env("BLOBSEER_HOME") + "/" + path + "/" + process + " " + \
        REMOTE_CFG_FILE + " >" + out + "/" + process + ".stdout 2>" + out + "/" + process + ".stderr&\""

def gen_kill(path, process):
    return "\"killall " + process + " || echo could not kill " + process + "\""

def gen_status(path, process):
    return "\"test `ps aux | grep " + path + "/" + process + " | grep -v grep | wc -l` -eq 1 || echo no " + process + " running\""

class RemoteCopy(threading.Thread):
    def __init__(self, n, src, dest):
        threading.Thread.__init__(self)
        self.node = n
        self.cmd = SCP + " " + src + " " + self.node + ":" + dest + " &>/dev/null"

    def run(self):
        p = subprocess.Popen(self.cmd, shell=True)
        if p.wait() != 0:
            print "[%s]: could not copy configuration file" % self.node

class RemoteLauncher(threading.Thread):
    def __init__(self, n, cmd):
        threading.Thread.__init__(self)
        self.node = n
        self.cmd = SSH + " " + self.node + " " + cmd
            
    def run(self):
        p = subprocess.Popen(self.cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (cout, cerr) = p.communicate()
        if cout.strip() != "":
            print "[%s]: %s" % (self.node, cout.strip())
            
(opts, args) = getopt.getopt(sys.argv[1:], "v:m:p:d:l:ks", ["vmgr=", "pmgr=", "providers=", "dht=", "launch=", "kill", "status"])
op = ""
machines = ""
template = ""
vmgr = ""
pmgr = ""
providers = []
dht = []
gencmd = gen_status
for (o, a) in opts:
    if o in ("-h", "--help"):
        usage()
        sys.exit(2)
    elif o in ("-v", "--vmgr"):
        vmgr = a
    elif o in ("-m", "--pmgr"):
        pmgr = a
    elif o in ("-p", "--providers"):
        providers = read_hosts(a)
    elif o in ("-d", "--dht"):
        dht = read_hosts(a)
    elif o in ("-l", "--launch"):
        gencmd = gen_launch
        f = open(a, "r")
        template = f.readlines()
        f.close()       
    elif o in ("-k", "--kill"):
        gencmd = gen_kill
    elif o in ("-s", "--status"):
        pass

if vmgr == "" or pmgr == "" or providers == [] or dht == []:
    usage()
    sys.exit(1)

workers = []

# If needed, copy all configuration files to the remote nodes
if (gencmd == gen_launch):
    gateways = ",\n\t\t".join(map(lambda x: "\"" + x + "\"", dht))
    f = open(TMP_CFG_FILE, "w")
    f.writelines(map(lambda x: x.replace("${gateways}", gateways).
                     replace("${vmanager}", "\"" + vmgr + "\"").
                     replace("${pmanager}", "\"" + pmgr + "\""), template))
    f.close()
    
    cfgs = set([vmgr]) | set([pmgr]) | set(providers) | set(dht)
    for x in cfgs:
        workers.append(RemoteCopy(x, TMP_CFG_FILE, REMOTE_CFG_FILE))

    for w in workers:
        w.start()

    for w in workers:
        w.join()
    
workers = []
workers.append(RemoteLauncher(vmgr, gencmd("vmanager", "vmanager")))
workers.append(RemoteLauncher(pmgr, gencmd("pmanager", "pmanager")))
for p in providers:
    workers.append(RemoteLauncher(p, gencmd("provider", "provider")))
for d in dht:
    workers.append(RemoteLauncher(d, gencmd("provider", "sdht")))
    
for w in workers:
    w.start()

for w in workers:
    w.join()

# Do some cleanup
try:
    os.remove(TMP_CFG_FILE)
except OSError:
    pass

print "All done!"
