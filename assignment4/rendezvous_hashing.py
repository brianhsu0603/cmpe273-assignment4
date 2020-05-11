import mmh3
import socket
import struct


class NodeRing():

    def __init__(self, nodes):
        assert len(nodes) > 0
        self.nodes = nodes

    def get_node(self, value):
     
     weights = [] 
     
     key = mhash(value)

     for node in self.nodes: 
      
       portNum = ip_to_num(node)
           
       w = self.weight(portNum, key)

       weights.append((w,node))

     _,node = max(weights)

     return node


    def weight(self,portNum,key):
    
     return (portNum + key) % len(self.nodes)

def ip_to_num(ip):
    
    packedIP = socket.inet_aton(ip)
    return struct.unpack("!L", packedIP)[0]


def mhash(key):
     
    return mmh3.hash(key)



def test():

 nodes = ["127.0.0.1", "127.0.0.2", "127.0.0.3"]
 data = ['a','b','c','d','e','1','2','3','4','5','abc','def','ghi','123','456','789']
 ring = NodeRing(nodes)
 
 for i in data:

  print (str(i) + " is sharded to server " + str(ring.get_node(i)))

   
test()


