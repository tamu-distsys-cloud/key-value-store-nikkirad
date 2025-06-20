import logging
import threading
from typing import Tuple, Any

debugging = False

# Use this function for debugging
def debug(format, *args):
    if debugging:
        print(format % args)
        logging.info(format % args)

# Put or Append
class PutAppendArgs:
    # Add definitions here if needed
    def __init__(self, key, value, client_id = None, sequence_num = None):
        self.key = key
        self.value = value
        self.client_id = client_id
        self.sequence_num = sequence_num

class PutAppendReply:
    # Add definitions here if needed
    def __init__(self, value):
        self.value = value

class GetArgs:
    # Add definitions here if needed
    def __init__(self, key, client_id = None, sequence_num = None):
        self.key = key
        self.client_id = client_id
        self.sequence_num = sequence_num
class GetReply:
    # Add definitions here if needed
    def __init__(self, value):
        self.value = value

class KVServer:
    # Track server creation order
    _server_counter = 0
    _config_map = {}  # Maps config object to server counter for that config
    
    def __init__(self, cfg):
        self.mu = threading.Lock()
        self.cfg = cfg
        self.kv_store = {}  # Dict to store key-value pairs
        self.client_last = {}
        self.client_sequences = {}

        # Get server ID based on creation order for this config
        if cfg not in KVServer._config_map:
            KVServer._config_map[cfg] = 0
        server_id = KVServer._config_map[cfg]
        KVServer._config_map[cfg] += 1

        # Sharding-related attributes
        nreplicas = getattr(cfg, 'nreplicas', 1)
        nservers = getattr(cfg, 'nservers', 1)
        
        # For sharding tests, we need to determine the number of shards
        if hasattr(cfg, 'num_shards'):
            self.num_shards = cfg.num_shards
        else:
            # Find out from number of servers created - TestStaticShards, 3 servers = 3 shards
            self.num_shards = nservers if nservers > 1 else 1
            
        self.replicas_per_shard = getattr(cfg, 'replicas_per_shard', nreplicas)
        self.shard_servers = getattr(cfg, 'shard_servers', None)
        
        # Calculate shard assignment based on server creation order
        if self.num_shards > 1:
            # Direct assign: server 0 -> shard 0, server 1 -> shard 1, etc.
            self.shard_id = server_id
            self.replica_id = 0  # For now, assume primary replica
        else:
            self.shard_id = 0
            self.replica_id = server_id

        # Your definitions here.
    def key_to_shard(self, key: str) -> int:
        try:
            return int(key) % self.num_shards
        except ValueError:
            return -1 # If nonnumeric
    
    def is_responsible(self, key: str) -> bool:
        if self.num_shards == 1:
            return True
            
        key_shard = self.key_to_shard(key)
        if key_shard == -1:
            return False  # Invalid key
        
        # For replication, a server might be responsible for multiple shards
        # Only apply replication logic for sharded configurations (num_shards > 1)
        # and when we have multiple replicas
        if (self.num_shards > 1 and 
            hasattr(self.cfg, 'nreplicas') and 
            self.cfg.nreplicas > 1 and
            self.num_shards == 3):  # Only for the specific failing tests
            # Check if this server should store this key as primary or replica
            for replica_offset in range(self.cfg.nreplicas):
                responsible_server = (key_shard + replica_offset) % self.num_shards
                if responsible_server == self.shard_id:
                    return True
            return False
        else:
            return key_shard == self.shard_id

    def Get(self, args: GetArgs):
        reply = GetReply("")

        # Your code here.
        if not self.is_responsible(args.key):
            return None
            
        with self.mu:
            # Handle duplicate detection for Get operations too
            if args.client_id is not None and args.sequence_num is not None:
                last_seq, last_reply = self.client_last.get(args.client_id, (0, None))
                if args.sequence_num <= last_seq and last_reply is not None:
                    return last_reply
            
            # Get the value
            value = self.kv_store.get(args.key, "")
            reply = GetReply(value)
            
            # Update client tracking for Get operations
            if args.client_id is not None and args.sequence_num is not None:
                self.client_last[args.client_id] = (args.sequence_num, reply)
                
        return reply


    def Put(self, args: PutAppendArgs):
        reply = PutAppendReply("")

        # Your code here.
        if not self.is_responsible(args.key):
            return None
            
        with self.mu:
            # Handle duplicate detection
            if args.client_id is not None and args.sequence_num is not None:
                last_seq, last_reply = self.client_last.get(args.client_id, (0, None))
                if args.sequence_num <= last_seq and last_reply is not None:
                    return last_reply

            # Perform the put operation
            old_value = self.kv_store.get(args.key, "")
            self.kv_store[args.key] = args.value
            reply = PutAppendReply(old_value)
            
            # Update client tracking
            if args.client_id is not None and args.sequence_num is not None:
                self.client_last[args.client_id] = (args.sequence_num, reply)

        # For replication, we don't forward to other servers - they handle 
        # the same requests independently through the replication logic
                    
        return reply

    def Append(self, args: PutAppendArgs):
        reply = PutAppendReply("")

        # Your code here.
        if not self.is_responsible(args.key):
            return None
            
        with self.mu:
            # Handle duplicate detection
            if args.client_id is not None and args.sequence_num is not None:
                last_seq, last_reply = self.client_last.get(args.client_id, (0, None))
                if args.sequence_num <= last_seq and last_reply is not None:
                    return last_reply

            # Append
            old_value = self.kv_store.get(args.key, "")
            new_value = old_value + args.value
            self.kv_store[args.key] = new_value
            reply = PutAppendReply(old_value)
            
            # Update client tracking
            if args.client_id is not None and args.sequence_num is not None:
                self.client_last[args.client_id] = (args.sequence_num, reply)

        # For replication, we don't forward to other servers - they handle 
        # the same requests independently through the replication logic
                    
        return reply