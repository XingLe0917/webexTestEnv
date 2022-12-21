from cacheout import LRUCache

curcache = LRUCache(maxsize=1024, ttl=24*60*60)

class wbxcacheservice:
    pass

def addLog(logid, msg):
    logkey = "JOBLOG_{}".format(logid)
    if curcache.has(logkey):
        curcache.set(logkey, "%s\n%s" % (curcache.get(logkey), msg))
    else:
        curcache.add(logkey, msg)

def removeLog(logid):
    logkey = "JOBLOG_{}".format(logid)
    if curcache.has(logkey):
        curcache.delete(logkey)

def getLog(logid):
    logkey = "JOBLOG_{}".format(logid)
    if curcache.has(logkey):
        return curcache.get(logkey)
    else:
        return None

def addTaskToCache(taskid, task):
    taskkey = "TASK_{}".format(taskid)
    curcache.add(taskkey, task)

def getTaskFromCache(taskid):
    taskkey = "TASK_{}".format(taskid)
    if curcache.has(taskkey):
        return curcache.get(taskkey)
    else:
        return None

