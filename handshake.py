import socket
import logging

logger = logging.getLogger(__name__)

class Handshake:
    def __init__(self, ip, port, info_hash, peer_id):
        self.ip = ip
        self.port = port
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.socket = None
        self.handshake = False

    def connect_with_peer(self):

        """
        Creates a socket with server and validates handshake message 
        """

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3)

        try:
            s.connect((self.ip, self.port))
            logger.debug(f"Successfully connected to {self.ip}, {self.port}")

        except socket.timeout:
            logger.debug(f"Connection timed out to {self.ip}") 
            s.close()  
            return False  
        
        except socket.error as e:
            logger.debug(f"Error: {e} to {self.ip}")
            s.close()
            return False

        pstr = b"BitTorrent protocol"
        pstrlen = bytes([len(pstr)])
        reserved = bytes(8)
        info_hash = self.info_hash
        peer_id = self.peer_id.encode("utf-8")
        handshake_message = pstrlen + pstr + reserved + info_hash + peer_id

        try:
            s.sendall(handshake_message)

        except socket.timeout:
            logger.debug(f"Timed out sending handshake to {self.ip}")
            s.close()
            return False
        
        except socket.error as e:
            logger.debug(f"Error occurred when sending handshake {e} to {self.ip}")
            s.close()
            return False
     
        try:
            reply =  s.recv(68)

        except socket.timeout:
            logger.debug(f"Timed out while recieving handshake from {self.ip}")
            s.close()
            return False    
        
        except socket.error as e:
            logger.debug(f"Handshake not recieved {e} from {self.ip}")
            s.close()
            return False

        if len(reply) != 68:
            logger.debug(f"Incorrect or incomplete handshake reply from {self.ip}")
            s.close()
            return False
        
        reply_pstr = reply[1:1 + reply[0]]
        reply_info_hash = reply[28:48]
        #reply_peer_id = reply[48:]
    
        if reply_pstr != pstr:
            logger.debug(f"Invalid handshake reply from {self.ip}")
            s.close()
            return False
        
        if reply_info_hash != info_hash:
            logger.debug(f"Info hash not matched from {self.ip}")
            s.close()
            return False
    
        self.handshake = True
        self.socket = s
        logger.debug(f"Handshake successful between {self.ip}")
        return True



    
    

        
        

