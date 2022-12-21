
from common.Config import Config

class wbxredis:
    rc = None

    def __init__(self):
        cfg = Config.getConfig()
        startup_nodes = cfg.getRedisClusterConnectionInfo()
        self.rc = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)

    @staticmethod
    def getRedis():
        if wbxredis.rc == None:
            wbxredis.rc = wbxredis()
        return wbxredis.rc

    def lpush(self, key, value):
        self.rc.lpush(key, value)

    def ltrim(self,key,cnt):
        self.rc.ltrim(0,cnt-1)

    def lrange(self,key,cnt):
        return self.rc.lrange(key, 0, cnt-1)

if __name__ == "__main__":
    # config = Config.getConfig()
    # startup_nodes = config.getRedisClusterConnectionInfo()
    # rc = StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)
    # rc.set("foo16706", "bar11")
    from common.wbxutil import wbxutil
    import datetime

    startdate = wbxutil.getcurrenttime(86400 * 30)
    enddate = wbxutil.getcurrenttime()
    delta = (enddate - startdate).days

    for i in range(delta):
        print(startdate + datetime.timedelta(days=i))