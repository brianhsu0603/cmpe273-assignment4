from bisect import bisect
from hashlib import md5

class NodeRing():
    
    def __init__(self, nodes, num_replicas = 2 ):
     
        replicas = generate_replicas(nodes, num_replicas)
        hash_replicas = [hash(replica) for replica in replicas]
        hash_replicas.sort()

        self.num_replicas = num_replicas
        self.replicas = replicas
        self.hash_replicas = hash_replicas
        self.replicas_map = {hash(replica): replica.split("-")[1] for replica in replicas}
    
    def get_node(self, value):
        
        pos = bisect(self.hash_replicas, hash(value))
        if pos == len(self.hash_replicas):
            return self.replicas_map[self.hash_replicas[0]]
        else:
            return str(self.replicas_map[self.hash_replicas[pos]])+" (virtual node)"



def generate_replicas(nodes, num_replicas):
        replicas = []
        for i in range(0,num_replicas):
            for server in nodes:
                
                replicas.append("{0}-{1}".format(i, server))
        return replicas

def hash(value):
    m = md5(value.encode('utf-8'))
    return int(m.hexdigest(), 16)

 


def test():

 nodes = ["127.0.0.1", "127.0.0.2", "127.0.0.3"]
 ring = NodeRing(nodes)
 data = ['a','b','c','d','e','1','2','3','4','5','abc','def','ghi','123','456','789']

 for i in data:

  print (str(i) + " is sharded to server " + str(ring.get_node(i)))



   
  
test()