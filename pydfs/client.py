import rpyc
import sys
import os
import ConfigParser

def send_to_minion(block_uuid,data,minions):
  print "sending: " + str(block_uuid) + str(minions)
  minion=minions[0]
  minions=minions[1:]
  host,port=minion

  con=rpyc.connect(host,port=port)
  minion = con.root.Minion()
  minion.put(block_uuid,data,minions)


def read_from_minion(block_uuid,minion):
  host,port = minion
  con=rpyc.connect(host,port=port)
  minion = con.root.Minion()
  return minion.get(block_uuid)

def get(master,fname,dest):
  file_table = master.get_file_table_entry(fname)
  if not file_table:
    print "404: file not found"
    return

  with open(dest, 'wb') as d:
    for block in file_table:
      for m in block[1]:
        try:
          data = read_from_minion(block[0],m)
        except:
          continue
        if data:
          #sys.stdout.write(data)
          d.write(data)
          break
      else:
          print "No blocks found. Possibly a corrupt file"

def put(master,source,dest):
  size = os.path.getsize(source)
  blocks = master.write(dest,size)
  with open(source, 'rb') as f:
    for b in blocks:
      data = f.read(master.get_block_size())
      block_uuid=b[0]
      minions = b[1]
      send_to_minion(block_uuid,data,minions)


def main(args):
  conf=ConfigParser.ConfigParser()
  conf.readfp(open('dfs.conf'))
  host = conf.get('master','host')
  port = int(conf.get('master','port'))

  con=rpyc.connect(host,port)
  master=con.root.Master()
  
  if args[0] == "get":
    get(master,args[1],args[2])
  elif args[0] == "put":
    put(master,args[1],args[2])
  else:
    print "try 'put srcFile destFile OR get file'"


if __name__ == "__main__":
  main(sys.argv[1:])
