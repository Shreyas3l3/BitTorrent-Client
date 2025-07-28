import logging 
from urllib.parse import urlparse
import urllib.parse
import requests
import bencodepy
import socket 
import struct
import random

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
    
    def generate_session_key(self):
        return random.randint(0, 0xFFFFFFFF)

    def send_connect_request(self):

        """
        This functions handles udp protocol to communicate with the server to get peer list
        """

        tracker_url = urlparse(self.announce)
        hostname = tracker_url.hostname
        port = tracker_url.port

        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) 
        server_address = (hostname, port)
        s.settimeout(7)

        protocol_id = 0x41727101980
        action = 0
        transaction_id = self.generate_session_key()
        connect_request_mssg = struct.pack("!QLL", protocol_id, action, transaction_id)

        for _ in range(3):
            try:
                s.sendto(connect_request_mssg, server_address)
                logger.debug(f"Sent connect request message to {server_address}")
            except socket.timeout:
                logger.debug(f"Timed out sending message to {server_address}")
                continue

            try:
                reply = s.recvfrom(16)[0]  
                logger.debug("Successfully received response") 
            except socket.timeout:
                logger.debug(f"Timed out receiving reply from {server_address}")     
                continue

            if len(reply) != 16:
                logger.debug("Reply was too short")
                continue
        
            elif struct.unpack(">I", reply[:4])[0] != 0:    
                logger.debug("Action field not 0")
                continue
        
            elif struct.unpack(">I", reply[4:8])[0] != transaction_id:
                logger.debug(f"Transaction id not matched, received {reply[4:8]} instead of {transaction_id}")  
                continue

            connection_id = struct.unpack(">Q",reply[8:16])[0]
            action = 1     
            event = 2   
            temp_ip = 0
            num_want = -1
            key = self.generate_session_key()
            announce_transaction_id = self.generate_session_key()

            announce_request = struct.pack(">QLL20s20sQQQLLLiH", 
                                        connection_id,
                                        action,
                                        announce_transaction_id, 
                                        self.info_hash,
                                        self.peer_id.encode("utf-8"),
                                        self.downloaded,
                                        self.left,
                                        self.uploaded,
                                        event,
                                        temp_ip,
                                        key,
                                        num_want,
                                        port,
                                      )
            try:
                s.sendto(announce_request, server_address)
                logger.debug("Sent announce request")
            except socket.timeout:
                logger.debug("Timed out sending announce request")
                continue

            try:
                response, address_two = s.recvfrom(4096) 
                logger.debug("Successfully received response")
            except socket.timeout:
                logger.debug("Timed out receiving announce request response")
                continue

            if len(response) < 20:
                logger.debug("No peer list received")    
                continue

            elif struct.unpack(">I", response[0:4])[0] != 1:
                logger.debug("Incorrect announce response")
                continue

            elif struct.unpack(">I", response[4:8])[0] != announce_transaction_id:   
                logger.debug("Transaction id does not match")
                continue

            logger.debug(f"Seeders available: {struct.unpack(">I", response[16:20])[0]}")
            return response[20:]
        
        s.close()
        return None

        
        
        
    

