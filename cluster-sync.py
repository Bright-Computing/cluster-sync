#!/usr/bin/python
# Copyright (c) 2004-2012 Bright Computing Holding BV. All Rights Reserved.
#
# This software is the confidential and proprietary information of
# Bright Computing Holding BV ("Confidential Information").  You shall not
# disclose such Confidential Information and shall use it only in
# accordance with the terms of the license agreement you entered into
# with Bright Computing Holding BV or its subsidiaries.

import sys, os, getopt, socket, time
import pythoncm, traceback

libraryPath = "/cm/local/apps/cmd/scripts/cloudproviders/amazon"
if os.path.exists(libraryPath):
  sys.path.append(libraryPath)
else:
  print("Library path not found: " + libraryPath)
  sys.exit(1)

from util import doPrint, debug, value
import util, kxml

logDir = "/var/spool/cmd/sync"
util.createDirIfNeeded(logDir)
util.logFile = "%s/sync.log" % logDir
syncXmlFile = None


def defaultExceptionHandler(excType, excValue, tb):
  doPrint("An internal error occured:\n  " + str(excValue) + ".\nFor details, see " + util.logFile)
  debug(excType)
  debug(excValue)
  for i in traceback.format_tb(tb):
    debug(i)
  sys.exit(0)

sys.excepthook = defaultExceptionHandler


def usage():
  print """
  Usage: %s -f <file> -x <exclude> [-v -d -n]

  Options
    -f | --file <file>  Synchronization definition file
    -v | --verbose     Be verbose in output 
    -x | --exclude <rsync exclude list file>  List of files that should be excluded from Rsync operations
    -d | --dry     Perform  a dry run
    -n | --preserve-fsmounts   Preserve FSMounts on target head node.
  """ % (sys.argv[0])
  sys.exit(1)


def translateType(type):
  type = type.lower()
  if type == "category":
    type = "Category"
  elif type == "network":
    type = "Network"
  elif type == "softwareimage":
    type = "SoftwareImage"
  elif type == "sgejobqueue":
    type = "SGEJobQueue"
  elif type == "pbsprojobqueue":
    type = "PbsProJobQueue"
  elif type == "openlavajobqueue":
    type = "OpenLavaJobQueue"
  elif type == "slurmjobqueue":
    type = "SlurmJobQueue"
  elif type == "monitorconfiguration":
    type = "MonConf"
  elif type == "metric":
    type = "Metric"
  elif type == "healthcheck":
    type = "HealthCheck"
  else:
    doPrint("Unknown type " + type)
    return None
  return type


def spaces(nr):
  return " ".ljust(nr, ' ')


def isFromCollection(cluster, object):
  className = ""
  if type(object) == type(pythoncm.Metric()):
    className = object.metricClass.name
  elif type(object) == type(pythoncm.HealthCheck()):
    className = object.healthCheckClass.name
  else:
    # Shouldn't arrive here actually
    return False
  if className == "PROTOTYPE":
    return False

  checks = cluster.getAll("HealthCheck")
  metrics = cluster.getAll("Metric")
  for m in metrics:
    if m.metricClass.name == "PROTOTYPE" and m.command == object.command:
      # TODO: fix issue with MonConf
      return False
      return m.name
  for c in checks:
    if c.healthCheckClass.name == "PROTOTYPE" and c.command == object.command:
      # TODO: fix issue with MonConf
      return False
      return c.name

  return False


def sync(srcCluster, dstCluster, action, dstSoftwareImages,test = False, indent = 4,doDryRun=0,myrsyncExcludeList=0,fsExcludeList=[]):
  typ = action["type"]
  typ = translateType(typ)
  srcObjName = action["src"]
  dstObjName = action["dest"]
  if test:
    debug(spaces(indent) + "Test local." + srcObjName + " -> " + dstCluster.name + "." + dstObjName)
  else:
    doPrint(spaces(indent) + "Syncing local." + typ + "." + srcObjName + " to " + dstCluster.name + "." + typ + "." + dstObjName)
  indent += 2    
  srcObject = srcCluster.find(srcObjName, typ)
  dstObject = dstCluster.find(dstObjName, typ)

  if not srcObject:
    doPrint(spaces(indent) + "local." + srcObjName + " not found")
    return False
  if not dstObject and not test:
    doPrint(spaces(indent) + dstCluster.name + "." + dstObjName + " will be created")
    
  srcPath = ""
  dstPath = ""
  if typ == "Category":
    newfslist = []
    
    if dstObject:
      newfslist = []
      remotefsMounts=dstObject.fsmounts
      doPrint(spaces(indent) + "Destination object contains FSMounts. I will preserve them.")
      newfslist=remotefsMounts
    else:
      newfslist = []
      if fsExcludeList:
        for fsobj in srcObject.fsmounts:
          if  not fsobj.mountpoint in fsExcludeList:
            newfslist+=[fsobj]
          else:
            continue
      else:
        newfslist=srcObject.fsmounts
    srcObject.fsmounts = newfslist 

  if typ == "SoftwareImage":
    if not dstObject:
      proposal = os.path.dirname(srcObject.path) + "/" + dstObjName
      if test:
        ok = util.getDefault(util.yesNoOptions, spaces(indent) + "Proposed path for " + name + "." + dstObjName + " is " + proposal + ". Do you want to continue? " + util.yesNoString["yes"], "yes")
        if util.equalish(ok, "n"):
          doPrint("Please create " + dstObjName + " on " + name + " with the path of your choice")
          doPrint("User aborted")
          sys.exit(0)
          
      dstPath = proposal
    else:
      dstPath = dstObject.path
    if test:
      # we will use this later in 'non-test' phase of this function
      dstSoftwareImages.append(dstPath)
    dstPath = [dstPath]
    srcPath = srcObject.path + "/"
  elif typ == "Metric" or typ == "HealthCheck":
    res = isFromCollection(srcCluster, srcObject)
    if res:
      if not test:
        doPrint(spaces(indent) + "Not processing " + srcObjName + ": it is derived from a metriccollection script (" + res + ")")
      return True
    srcPath = srcObject.command
    dstPath = []
    for path in dstSoftwareImages:
      dstPath.append(path + srcPath)
  else:
    srcPath = None

  if test:
    return True

  if type(srcPath) == type(""): # for a metric, healthcheck or software image, some files have to be copied. Done here:
    if not os.path.exists(srcPath):
      # Also metrics and health checks are taken from the headnodes root directory
      doPrint(spaces(indent) + "Path defined in local." + srcObjName + " not found: " + srcPath)
      return False
    else:
      debug(spaces(indent) + "Using " + srcPath + " as source of local." + srcObjName)    

    # ready to copy data to remote
    for dest in dstPath:
      if typ == "SoftwareImage":
        rsyncLogfile = "%s/rsync.%s" % (logDir, dstCluster.name)
        doPrint(spaces(indent) + "Syncing " + srcPath + " to " + dstCluster.host + ":" + dest + ". For progress, see " + rsyncLogfile)
        if not (myrsyncExcludeList == ""):
          doPrint(spaces(indent) + "Using RSYNC exclude list " + myrsyncExcludeList)
          cmd = "rsync -a --numeric-ids --force --log-file='%s' --delete --rsh=ssh --exclude-from='%s'  --force " % (rsyncLogfile, myrsyncExcludeList)
        else:
          cmd = "rsync -a --numeric-ids --force --log-file='%s' --delete --rsh=ssh --force " % (rsyncLogfile)
        if doDryRun:
          cmd = cmd + " -n "
           
          doPrint(spaces(indent) + "Performing dry RSYNC run.")
        else:
          doPrint(spaces(indent) + "Performing RSYNC run.")
        doPrint(spaces(indent) + "Exclude list is: " + myrsyncExcludeList)
        cmd = cmd + " '%s' '%s':'%s'" % ( srcPath, dstCluster.host, dest)
        util.execute(cmd)
        if util.exitcode:
          doPrint(spaces(indent) + "Error copying " + srcPath + " to " + dstCluster.name)
          return False
        else:
          debug(spaces(indent) + "copying " + srcPath + " to " + dstCluster.name + " succeeded")
        dstImagePath = dest
        cmd = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null '%s' \"rm -rf '%s'/root/.ssh; cp -R /root/.ssh '%s'/root \"" % (dstCluster.host, dest, dest)
        util.execute(cmd)
        if util.exitcode:
          doPrint(spaces(indent) + "Error restoring SSH keys")
          return False
        else:
          debug(spaces(indent) + "Restoring SSH keys succeeded")

        doPrint(spaces(indent) + "Restoring superuser SSH keys")
        debug(spaces(indent) + "Excuting " + cmd)
        pass
      else:
        rsyncLogfile = "%s/rsync.%s" % (logDir, dstCluster.name)
        cmd = "rsync -a --numeric-ids --force --log-file='%s' --delete '%s' '%s':'%s'" % (rsyncLogfile, srcPath, dstCluster.host, dest)
        util.execute(cmd)
        if util.exitcode:
          doPrint(spaces(indent) + "Error copying " + srcPath + " to " + dstCluster.name)
          return False
        else:
          debug(spaces(indent) + "copying " + srcPath + " to " + dstCluster.name + " succeeded")

  if dstObject: # synchronize
    properties = srcObject.getProperties(False)
    debug(spaces(indent) + "Going to sync local." + srcObjName + " -> " + dstCluster.name + "." + dstObjName)
    syncRes = dstObject.synchronizeFrom(srcObject, properties)
    if typ == "Category":
      dstObject.fsmounts=newfslist
    if not syncRes:
      doPrint(spaces(indent) + "Syncing local." + srcObjName + " -> " + dstCluster.name + "." + dstObjName + " failed")
      err = srcCluster.getLastError()
      if err:
        doPrint(spaces(indent + 2) + err)
        err = dstCluster.getLastError()
      if err:
        doPrint(spaces(indent + 2) + err)
      return False
    else:
      debug(spaces(indent) + "Synced local." + srcObjName + " -> " + dstCluster.name + "." + dstObjName)

  else: # no such object at remote side. Clone it
    debug(spaces(indent) + "Going to clone local." + srcObjName + " -> " + dstCluster.name + "." + dstObjName)
    dstObject = srcObject.clone()
    if not dstObject:
      doPrint(spaces(indent) + "Cloning local." + srcObjName + " -> " + dstCluster.name + "." + dstObjName + " failed")
      err = srcCluster.getLastError()
      if err:
        doPrint(spaces(indent + 2) + err)
        err = dstCluster.getLastError()
      if err:
        doPrint(spaces(indent + 2) + err)
      return False
    dstObject.name = dstObjName
    if typ == "SoftwareImage":
      dstObject.path = dstPath[0]
    res = dstCluster.add(dstObject)
    if not res:
      doPrint(spaces(indent) + "Adding local." + srcObjName + " -> " + dstCluster.name + "." + dstObjName + " failed")
      err = srcCluster.getLastError()
      if err:
        doPrint(spaces(indent + 2) + err)
        err = dstCluster.getLastError()
      if err:
        doPrint(spaces(indent + 2) + err)
      return False
    syncRes = True

  # dstObject is ready (sync'ed or cloned), we can now commit it
  debug(spaces(indent) + "Going to commit " + dstCluster.name + "." + dstObjName)
  commitRes = dstObject.commit()
  if not commitRes.result or commitRes.count:
    if not commitRes.result:
      doPrint(spaces(indent) + "Error committing " + dstCluster.name + "." + dstObjName)
    else:
      doPrint(spaces(indent) + "Warning when committing " + dstCluster.name + "." + dstObjName)
    for j in range(commitRes.count):
      doPrint(spaces(indent + 2) + commitRes.getValidation(j).msg)
  string = "Committed local." + srcObjName + " -> " + dstCluster.name + "." + dstObjName
  string += ": sync=" + str(syncRes) + ", commit=" + str(commitRes.result != 0)
  debug(spaces(indent) + string)
  return commitRes.result != 0


def fatal(msg):
  doPrint(msg)
  sys.exit(1)


def tcpPing(host, port):
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  port = int(port)
  try:
    s.connect((host, port))
    s.shutdown(2)
    return True
  except:
    return False
  return False


def mySort(actions, order):
  # Sorting the "sync" actions by object type.
  temp = []
  if type(actions) != type([]):
    actions = [actions]
  for typ in order:
    for action in actions:
      if not value(action, "name").lower() == "sync":
        continue
      if value(action, "type").lower() == typ:
        temp.append(action)
  if len(temp) != len(actions):
    doPrint("Something went wrong while sorting the actions")
    sys.exit(1)
  return temp
  

try:
  options, arguments = getopt.getopt(sys.argv[1:], 'f:vx:dn', ['file', 'verbose','exclude','dry','preserve-fsmounts'])
  if len(options) == 0:
    usage()
except getopt.GetoptError, err:
  doPrint(str(err))
  sys.exit(1)
for opt, arg in options:
  if opt in ('-h', '--help'):
    usage()
  elif opt in ('-f', '--file'):
    syncXmlFile = arg
  elif opt in ('-v', '--verbose'):
    util.verbose = 1
  elif opt in ('-x', '--exclude'):
    global _rsyncExcludeList
    _rsyncExcludeList = arg
  elif opt in ('-d', '--dry'):
    global _doDryRun
    _doDryRun = 1
  elif opt in ('-n', '--preserve-fsmounts'):
    global preserveMounts
    preserveMounts = 1

if not syncXmlFile:
  usage()

now = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
debug("----------%s-----------------" % now)
doPrint("For a detailed log file, see: " + util.logFile)
    
debug("Sync definition file: " + syncXmlFile)

if not os.path.exists(syncXmlFile):
  fatal("Error: file " + syncXmlFile + " not found")
  
xml = util.readFileToStr(syncXmlFile)
syncdef = kxml.deserialize(xml)
remoteClusters = value(syncdef, "cluster")
localCluster = value(syncdef, "local")

###### Sanity checks, mainly for things regarding to connecting to localhost:
if type(remoteClusters) == type({}):
  remoteClusters = [remoteClusters]
if type(remoteClusters) != type([]):
  fatal("Error: no remote clusters in definition found")
if not localCluster:
  fatal("Error: no local cluster definition found")
brightport = value(localCluster, "brightport")
if not brightport:
  fatal("Error: no port found to connect to local cmdaemon")
host = value(localCluster, "host")
if not host:
  fatal("Error: no host found to connect to local cmdaemon")
cmdaemonurl = "https://" + host + ":" + str(brightport)
if not tcpPing(host, brightport):
  fatal("Unable to connect to local cmdaemon at " + cmdaemonurl)
pemfile = value(localCluster, ["brightcert", "pemfile"])
if not pemfile or not os.path.exists(pemfile):
  fatal("Error: no pem file found to connect to local cmdaemon")
keyfile = value(localCluster, ["brightcert", "keyfile"])
if not keyfile or not os.path.exists(keyfile):
  fatal("Error: no key file found to connect to local cmdaemon")


clustermanager = pythoncm.ClusterManager()

localCluster = clustermanager.addCluster(cmdaemonurl, pemfile, keyfile)
if not localCluster.connect():
  doPrint("Error: unable to connect to " + cmdaemonurl)
  fatal(localCluster.getLastError())


#### Sanity checks on cluster definitions
for cluster in remoteClusters:
  name = value(cluster, "name")
  debug("Testing " + name)
  error = False
  if not name:
    doPrint("Error: no name found for cluster definition")
    error = True
  brightport = value(cluster, "brightport")
  if not brightport:
    doPrint("Error: no port found to connect to cmdaemon of " + name)
    error = True
  sshport = value(cluster, "sshport")
  if not sshport:
    doPrint("Error: no port found to connect to sshd of " + name)
    error = True
  host = value(cluster, "host")
  if not host:
    doPrint("Error: no host found to connect to remote cmdaemon of " + name)
    error = True
  pemfile = value(cluster, ["brightcert", "pemfile"])
  if not pemfile or not os.path.exists(pemfile):
    doPrint("Error: no pem file found to connect to " + name)
    error = True
  keyfile = value(cluster, ["brightcert", "keyfile"])
  if not keyfile or not os.path.exists(keyfile):
    doPrint("Error: no key file found to connect to " + name)
    error = True
  if not tcpPing(host, brightport):
    doPrint("Unable to connect to " + name + " at " + host + ":" + str(brightport))
    error = True
  if not tcpPing(host, sshport):
    doPrint("Unable to connect to " + name + " at " + host + ":" + str(sshport))
    error = True
  cmd = "/usr/bin/ssh -q -p %s -o PasswordAuthentication=no %s hostname" % (sshport, host)
  res = util.execute(cmd, False)
  if util.exitcode:
    doPrint("Error connecting to sshd of " + name)
    doPrint("Connection should not need password authentication")
    error = True
  if error == True:
    ok = util.getDefault(util.yesNoOptions, "Error preprocessing " + name + ". Do you want to continue? " + util.yesNoString["yes"], "yes")
    if util.equalish(ok, "n"):
      doPrint("User aborted")
      sys.exit(0)    
    remoteClusters.remove(cluster)
    doPrint("Will not process " + name)
  else:
    debug("All seems ok for: " + name)


for cluster in remoteClusters:
  name = value(cluster, "name")
  brightport = value(cluster, "brightport")
  sshport = value(cluster, "sshport")
  host = value(cluster, "host")
  doPrint("")
  doPrint(name)
  cmdaemonurl = "https://" + host + ":" + str(brightport)
  pemfile = value(cluster, ["brightcert", "pemfile"])
  keyfile = value(cluster, ["brightcert", "keyfile"])
  fslist  = value(cluster, ["fsexclude"]) 
  if not fslist:
    fslist = []
  doPrint(spaces(2) + "The following list of FSMounts will not be synchronized")
  doPrint(spaces(2) +  ', '.join(fslist) )
  doPrint(spaces(2) + "")
  debug(spaces(2) + "Connecting to " + name + " at " + cmdaemonurl + " using " + pemfile + " and " + keyfile)
  connection = clustermanager.addCluster(cmdaemonurl, pemfile, keyfile)
  if not connection.connect():
    doPrint(spaces(2) + "Error: unable to connect to " + name)
    doPrint(connection.getLastError())
    ok = util.getDefault(util.yesNoOptions, "Error preprocessing " + name + ". Do you want to continue? " + util.yesNoString["yes"], "yes")
    if util.equalish(ok, "n"):
      doPrint("User aborted")
      sys.exit(0)     
    continue
  connection.name = name
  connection.host = host
  connection.brightport = brightport
  connection.sshport = sshport
  
  doPrint(spaces(2) + "Processing synchronization actions for " + name)
  actions = value(cluster, "action", [])
  actions = mySort(actions, ["softwareimage", "network", "category", "sgejobqueue", "slurmjobqueue", "pbsprojobqueue", "openlavajobqueue", "metric", "healthcheck", "monitorconfiguration"])

  try:
    _rsyncExcludeList
  except NameError:
    _rsyncExcludeList=""
  else:
    doPrint(spaces(2) + "The exclude list is " + _rsyncExcludeList)
  try:
    _doDryRun
  except NameError:
    _doDryRun=0
    doPrint(spaces(2) + "Will not perform a dry run.")
  else:
    _doDryRun=1
    doPrint(spaces(2) + "Performing dry run. -d option selected")
  try:
    preserveMounts
  except NameError:
    preserveMounts=0
  else:
    doPrint(spaces(2) + "Will attempt to preserve already defined FSMounts")

  #### Test phase, whether all requested objects are valid
  ok = True
  dstSoftwareImages = [] # will be used set in test phase, and used in next stage
  for action in actions:
    if action["name"] == "sync":
      ok = ok & sync(localCluster, connection, action, dstSoftwareImages, True,4,0,_rsyncExcludeList)
    else:
      doPrint(spaces(4) + "Error: action " + action["name"] + " is not supported")
      ok = False
  if not ok:
    doPrint(spaces(2) + "Errors occured while preparing synchronizing of " + name)

  doPrint(spaces(2) + "Argument List: " +  str(sys.argv) )
  #### Next phase, actual synchronization can now continue
  ok = True
  for action in actions:
    if action["name"] == "sync":
      ok = ok & sync(localCluster, connection, action, dstSoftwareImages,False,4,_doDryRun,_rsyncExcludeList,fslist)
    else:
      doPrint(spaces(4) + "Error: action " + action["name"] + " is not supported")
  if ok:
    doPrint(spaces(2) + "Synchronization of " + name + " succeeded")
  else:
    doPrint(spaces(2) + "Errors occured while synchronizing " + name)
    
  if not connection.disconnect():
    debug(spaces(2) + "Waring: disconnect from " + name + " failed")


if not localCluster.disconnect():
  debug("Waring: disconnect from local cluster failed")
