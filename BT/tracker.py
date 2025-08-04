import logging 
import urllib.parse
import requests
import bencodepy
import socket 
import struct

logger = logging.getLogger(__name__)

class Tracker:
    def __init__(self, announce_url, info_hash, peer_id, port, file_length , http):
        self.announce = announce_url
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.port = port
        self.downloaded = 0
        self.uploaded = 0
        self.left = file_length
        self.compact = 1
        self.http = http

    def get_encoded_info_hash(self):
        return urllib.parse.quote_from_bytes(self.info_hash)

    def get_encoded_peer_id(self):
        return urllib.parse.quote_from_bytes(self.peer_id.encode("utf-8"))
    
    def get_parameters(self):
        return {
            "info_hash": self.get_encoded_info_hash(),
            "peer_id": self.get_encoded_peer_id(),  
            "port": self.port,
            "uploaded": 0,
            "downloaded": 0,
            "left": self.left,
            "compact": 1,
            "event": "started"
        }
    
    def send_request(self):

        """
        Request peer list from tracker
        """

        param = Tracker.get_parameters(self)
        param = urllib.parse.urlencode(param, safe="%_")    
        tracker_url = self.announce + "?" + param
        r = requests.get(tracker_url)
        encoded_return_info = bencodepy.decode(r.content)
        return encoded_return_info[b'peers']
        
    def decode_peer_list(self, encoded_peer):
        list_of_peers = []
        for i in range(0, len(encoded_peer), 6):
            ip = socket.inet_ntoa(encoded_peer[i:i+4])
            tracker_port = struct.unpack(">H", encoded_peer[i+4:i+6])[0]
            list_of_peers.append((ip,tracker_port))
        return list_of_peers 
    


        
        
        
    

