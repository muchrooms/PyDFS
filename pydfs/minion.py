import rpyc
import uuid
import os
import ConfigParser

from rpyc.utils.server import ThreadedServer

DATA_DIR="/tmp/minion/"

class MinionService(rpyc.Service):
  class exposed_Minion():
    blocks = {}

    def exposed_put(self,block_uuid,data,minions):
      with open(DATA_DIR+str(block_uuid),'wb') as f:
        f.write(data)
      if len(minions)>0:
        self.forward(block_uuid,data,minions)


    def exposed_get(self,block_uuid):
      block_addr=DATA_DIR+str(block_uuid)
      if not os.path.isfile(block_addr):
        return None
      with open(block_addr, 'rb') as f:
        return f.read()   
 
    def forward(self,block_uuid,data,minions):
      print "8888: forwaring to:"
      print block_uuid, minions
      minion=minions[0]
      minions=minions[1:]
      host,port=minion

      con=rpyc.connect(host,port=port)
      minion = con.root.Minion()
      minion.put(block_uuid,data,minions)

    def delete_block(self,uuid):
      pass

def int_handler(signal, frame):
  master.minion_exit(minion)
  sys.exit(0)

if __name__ == "__main__":
  conf=ConfigParser.ConfigParser()
  conf.readfp(open('dfs.conf'))
  host = conf.get('master','host')
  port = int(conf.get('master','port'))
  con=rpyc.connect(host,port=port)

  import sys
  ip=sys.argv[1]
  p=int(sys.argv[2])

  minion = (ip, p)
  master=con.root.Master()
  master.minion_enter(minion)
  
  import signal
  signal.signal(signal.SIGINT, int_handler)

  DATA_DIR = DATA_DIR + sys.argv[1] + '/' + sys.argv[2] + '/'
  #if not os.path.isdir(DATA_DIR): os.mkdir(DATA_DIR)
  if not os.path.isdir(DATA_DIR): os.makedirs(DATA_DIR)
  t = ThreadedServer(MinionService, hostname=ip, port=p)
  t.start()
