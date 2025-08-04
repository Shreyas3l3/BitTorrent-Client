import logging
import asyncio

logger = logging.getLogger(__name__)

class Handshake:
    def __init__(self, ip, port, info_hash, peer_id):
        self.ip = ip
        self.port = port
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.handshake = False
        self.writer = None
        self.reader = None

    async def connect_with_peer(self):

        """
        Creates a socket with server and validates handshake message 
        """

        writer = None
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(self.ip, self.port), timeout=3)
            self.writer = writer
            self.reader = reader
            logger.debug(f"Successfully connected to {self.ip}, {self.port}")

        except Exception as e:
            logger.debug(f"Error: {e} to {self.ip}")
        except asyncio.TimeoutError:
            logger.debug(f"Connection timed out to {self.ip}")
        if writer is None:
            return False         

        pstr = b"BitTorrent protocol"
        pstrlen = bytes([len(pstr)])
        reserved = bytes(8)
        info_hash = self.info_hash
        peer_id = self.peer_id.encode("utf-8")
        handshake_message = pstrlen + pstr + reserved + info_hash + peer_id

        try:
            writer.write(handshake_message)
            await writer.drain()
        except Exception as e:
            logger.debug(f"Error: {e} to {self.ip}")
            await self.close_writer(writer)
            return False
     
        try:
            reply = await reader.read(68)

        except Exception as e:
            logger.debug(f"Error: {e} to {self.ip}")
            await self.close_writer(writer) 
            return False

        if len(reply) != 68:
            logger.debug(f"Incorrect or incomplete handshake reply from {self.ip}")
            await self.close_writer(writer) 
            return False
        
        reply_pstr = reply[1:1 + reply[0]]
        reply_info_hash = reply[28:48]
        #reply_peer_id = reply[48:]
    
        if reply_pstr != pstr:
            logger.debug(f"Invalid handshake reply from {self.ip}")
            await self.close_writer(writer)  
            return False
        
        if reply_info_hash != info_hash:
            logger.debug(f"Info hash not matched from {self.ip}")
            await self.close_writer(writer)
            return False
    
        self.handshake = True
        logger.debug(f"Handshake successful between {self.ip}")
        return True

    async def close_writer(self, writer):

        """
        Closes writer safely
        """

        try:
            writer.close()
            await writer.wait_closed()
        except Exception as e:
            logger.debug(f"Error while closing connection to {self.ip}: {e}")

    
    

        
        

