import redis

class MyRedis():
    def __init__(self,host,port,password):
        pool = redis.ConnectionPool(host=host, port=port, password=password)
        self.r = redis.Redis(connection_pool=pool)

    def set(self, name, value):
        return self.r.set(name=name, value=value)

    def get(self, name):
        if self.r.exists(name):
            return self.r.get(name)
        else:
            return None





