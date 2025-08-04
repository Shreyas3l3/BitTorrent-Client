import math
import hashlib
import logging
import asyncio

logger = logging.getLogger(__name__)

class exchange:
    def __init__(self, info_hash, peer_id, ip, piece_length, total_pieces, last_piece_length, piece_manager, torrent, writer, reader):
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.ip = ip

        self.pieces_peer_has = set()
        self.requested_blocks = {}

        self.piece_length = piece_length
        self.total_pieces = total_pieces
        self.last_piece_length = last_piece_length
        self.piece_manager = piece_manager
        self.torrent = torrent
    
        self.writer = writer
        self.reader = reader

        self.connection_failed = False
        self.consecutive_failures = 0
        self.max_consecutive_failures = 3



    async def receive_message(self):

        """ 
        Receive bitfield message from peer after handshake which tells us which pieces peer has
        """

        buffer = bytearray()
        try:
            # Keep reading until you get first 4 bytes = message length prefix 
            while len(buffer) < 4:
                chunk = await asyncio.wait_for(self.reader.read(4096), timeout=5)
                if not chunk:
                    return None
                buffer += chunk  

            mssg_length = int.from_bytes(buffer[:4], 'big')

            # Keep reading until we have the entire message
            while len(buffer) < mssg_length + 4:
                chunk = await asyncio.wait_for(self.reader.read(4096), timeout=5)

                if not chunk:
                    return None
                buffer += chunk

            message = buffer[4: 4 + mssg_length]

            if mssg_length == 0:
                return None
        
            message_id = message[0]
            content = message[1:]

            # id should be 5 for bitfield and content is what pieces peer has (first call)
            # id should be 7 for message and then content is message
            return {"id": message_id, "content": content}
        
        except Exception as e:
            logger.debug(f"Socket timed out in receive message function {e}")
            self.consecutive_failures += 1
            if self.consecutive_failures >= self.max_consecutive_failures:
                self.connection_failed = True
            return None
  
    
    def parse_message(self, content, limit_piece):

        """
        Parse btifield message and store which pieces peer has (bit-shift)
        """

        pieces_peer_has = set()
        index = 0

        for byte in content:
            for bit in range(7, -1, -1):
                if index >= limit_piece:
                    break
                if ((byte >> bit) & 1) == 1:
                    pieces_peer_has.add(index)
                index += 1   
        self.pieces_peer_has = pieces_peer_has
        if len(self.pieces_peer_has) == self.total_pieces:
            logger.debug(f"This peer has all {self.total_pieces} pieces")

    def get_interested_message(self):
        return (1).to_bytes(4, 'big') + (2).to_bytes(1, 'big')
    
    def get_unchoke_message(self):
        return (1).to_bytes(4, 'big') + (21).to_bytes(1, 'big')
    
    async def decide_interest(self):

        """
        Check if peer has pieces we don't have and then send interested message and wait for unchoke message
        """
        have_pieces = set()
        async with self.piece_manager.lock:
            have_pieces = self.piece_manager.have_pieces.copy()

        pieces_needed = self.pieces_peer_has - have_pieces

        if pieces_needed:
            try:
                self.writer.write(self.get_interested_message())
                await self.writer.drain()  # type: ignore

            except Exception as e:
                logger.debug(f"Error sending interest to: {self.ip}: {e}")
                self.writer.close()
                await self.writer.wait_closed()  
                return False

            try: 
                response = await self.reader.read(4096)
                
            except Exception as e:
                logger.debug(f"Error receiving response: {e} to {self.ip}")
                self.writer.close()
                await self.writer.wait_closed()  
                return False
            
            if len(response) >= 5 and response[4] == 1:
                logger.debug("Peer has unchoked you.")
                return True 
            
        logger.debug("This peer has no pieces we need")
        return False
    
    async def request_block(self, piece_index, offset, block_size):

        """
        Send request block message to peer
        """

        length = (13).to_bytes(4, byteorder='big')
        id = (6).to_bytes(1, byteorder='big')
        index_bytes = piece_index.to_bytes(4, byteorder='big')
        offs = offset.to_bytes(4, byteorder='big')
        block_length = block_size.to_bytes(4, byteorder='big')

        request_mssg = length + id + index_bytes + offs + block_length

        try:
            self.writer.write(request_mssg)
            await self.writer.drain()
            logger.debug(f"Block requested from {self.ip}")
        except Exception as e:
            logger.debug(f"Error while requesting piece {e}") 
            self.consecutive_failures += 1
            if self.consecutive_failures >= self.max_consecutive_failures:
                self.connection_failed = True
            raise 
    
    def get_piece_message(self, content, block_num):

        """
        Puts block message and block offset in nested dict corresponding to piece_index (piece number)
        """ 

        if content is None:
            logger.debug(f"Peer sent None {self.ip}")

        if len(content) < 8:
            logger.debug(f"Incomplete message from {self.ip}") 
            logger.debug(content)
            return

        piece_index = int.from_bytes(content[:4], byteorder='big') 
        block_offset = int.from_bytes(content[4:8], byteorder='big') 
        block_data = content[8:] 

        if piece_index not in self.requested_blocks:
            self.requested_blocks[piece_index] = {}

        self.requested_blocks[piece_index][block_offset] = block_data  
        logger.debug(f"Received block {block_num} of piece {piece_index} from {self.ip}") 

    async def get_all_pieces(self):

        """
        Handles the looping of common pieces and blocks which make up the piece
        """

        block_size = 16384

        while True:

            missing_pieces = await self.piece_manager.get_missing_pieces()
            common = self.pieces_peer_has & missing_pieces

            if self.connection_failed:
                logger.debug(f"Connection to {self.ip} has failed too many times, skipping peer")
                return False

            if not common:
                logger.debug(f"No more pieces from {self.ip} that we need")
                return False
            
            one_piece_completed = False

            for piece_index in sorted(common):
                    
                    if await self.piece_manager.is_piece_complete(piece_index): 
                        continue

                    if await self.piece_manager.is_piece_downloading(piece_index):
                        continue

                    is_last_piece = piece_index == self.total_pieces - 1
                    current_piece_length = self.last_piece_length if is_last_piece else self.piece_length
                    total_blocks = math.ceil(current_piece_length / block_size)
                    self.requested_blocks[piece_index] = {}
                    success = True

                    for i in range(total_blocks):
                        offset = i * block_size
                        current_block = min(block_size, current_piece_length - offset)
                        retries = 3
                        received = False

                        while retries > 0 and not received:
                            try:
                                await self.request_block(piece_index, offset, current_block)  
                                response = await self.receive_message()  

                                if response is None:
                                    retries -= 1
                                    logger.debug(f"No response for block {i} of piece {piece_index} from {self.ip}")
                                    continue

                                if response["id"] == 7:
                                    self.get_piece_message(response["content"], i)
                                    if piece_index in self.requested_blocks and offset in self.requested_blocks[piece_index]:
                                        received = True
                                else:
                                    logger.debug("Incorrect message id from peer during block receiving")
                                    retries -= 1

                            except asyncio.TimeoutError:
                                retries -= 1
                                logger.debug(f"Timed out getting block {i} of piece {piece_index} from {self.ip}")

                            except (ConnectionError, BrokenPipeError, OSError) as e:
                                logger.error(f"Connection error with {self.ip}: {e}")
                                self.connection_failed = True
                                success = False
                                break  

                            except Exception as e:
                                retries = 0
                                logger.error(f"Unexpected error on block {i} of piece {piece_index}: {e}")
                                break

                        if not received:
                            logger.debug(f"Block {i} of piece {piece_index} failed from {self.ip}")
                            success = False
                            break

                    if not success:
                        async with self.piece_manager.lock:
                            await self.piece_manager.piece_failed(piece_index)
                        continue  

                    got_blocks = self.requested_blocks[piece_index]
                    if len(got_blocks) != total_blocks:
                        logger.debug(f"Piece {piece_index} incomplete from {self.ip}")
                        async with self.piece_manager.lock:
                            await self.piece_manager.piece_failed(piece_index)
                        continue

                    full_piece_data = bytearray()
                    for j in range(total_blocks):
                        full_piece_data += got_blocks[j * block_size]

                    if self.verify_piece(piece_index, full_piece_data):
                        try:
                            await asyncio.wait_for(self.piece_manager.piece_complete(piece_index, full_piece_data), timeout=30)
                            logger.debug(f"Completed piece {piece_index}\n")
                            one_piece_completed = True
                        except asyncio.TimeoutError:
                            logger.debug(f"Piece {piece_index} timed out in piece complete function")
                            async with self.piece_manager.lock:
                                await self.piece_manager.piece_failed(piece_index)
                                continue  

                    else:
                        logger.debug(f"Piece {piece_index} failed hash check")
                        async with self.piece_manager.lock:
                            await self.piece_manager.piece_failed(piece_index)

            if not one_piece_completed and self.consecutive_failures > 0:
                logger.debug(f"No progress made with {self.ip}, abandoning peer")
                return False
            
            if await self.piece_manager.is_download_complete():
                return True

 
    def verify_piece(self, piece_index, data):

        """
        Ensures expected hash of piece from torrent matches hash of piece we receive
        """

        expected_hash = self.torrent.get_piece_hashes()
        sha1 = hashlib.sha1()
        sha1.update(data)
        actual_hash = sha1.digest()
        if actual_hash == expected_hash[piece_index]:
            logger.debug(f"\nHash matches")
            return True
        logger.debug("Hash not matching")
        return False


            