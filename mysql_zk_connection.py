import MySQLdb as mdb
import logging
import json
from kazoo.recipe.lock import ZooLock
from kazoo.exceptions import NoNodeException

class MysqlZkConnection():
    def __init__self(self, zk_client, max_attempts = 5, **kwargs):
        self.zk = zl_client
        self.host = None
        self.port = None
        self.max_attempts = max_attempts
        master = self.zk.get("/mysql/master")[0]
        if master:
            host, port = json.loads(master)['address'].split(';')
        else:
            master = self.elect_master()
            host, port = master['address'].split(';')

        kwargs['host'] = host
        kwargs['port'] = int(port)
        self.kwargs = kwargs

    def connect(self):
        for i in range(self.max_attempts):
            try:
                return mdb.connect(**self.kwargs)
            except mdb.Error, e:
                if i == self.max_attempts - 1:
                    master = self.zk.get("/mysql/master")[0]
                    host, port = json.loads(master)['address'].split(':')
                    port = int(port)
                    if host == self.host and port == self.port:
                        master = self.elect_master()
                        if master is False:
                            raise
                        host, port = master['address'].split(':')
                        port = int(port)
                    self.update(host = host, port = port)
                    return self.connect()
                
    def elect_master(self):
        lock = ZooLock(self.zk, '/mysql/election')
        with lock:
            master = self.atomic_elect_master()
        return master

    def atomic_elect_master(self):
        zk = self.zk
        data = zk.get("/mysql/master")
        if data[0]:
            data = json.loads(data[0])
        else:
            data = {}
        if 'address' in data.keys():
            host, port = data['address'].split(':')
            port = int(port)
        if (not 'address' in data.keys()) or (host == self.host and port == self.port):
            if 'address' in data.keys():
                try:
                    zk.delete('/mysql/providers/' + data['address'])
                except NoNodeException:
                    pass
                
            addresses = zk.get_children('mysql/providers')
            master = addresses[0]
            host, port = master.split(':')
            port = int(port)
            zk.set('/mysql/master', json.dumps({u'address' : master}))
            master = {'address' : master}
        else:
            master = data
        return master

    def update(self,**kwargs):
        for key, value in kwargs.iteritems():
            setattr(self, key, value)
