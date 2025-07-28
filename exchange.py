import socket
import math
import hashlib
import logging

logger = logging.getLogger(__name__)

class exchange:
    def __init__(self, socket, info_hash, peer_id, ip, piece_length, total_pieces, last_piece_length, piece_manager, torrent):
        self.socket = socket
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.ip = ip

        self.peer_choking = True
        self.peer_interested = False
        self.choking = True
        self.interested = False

        self.pieces_peer_has = set()
        self.requested_pieces = set()
        self.requested_blocks = {}

        self.piece_length = piece_length
        self.total_pieces = total_pieces
        self.last_piece_length = last_piece_length
        self.piece_manager = piece_manager
        self.torrent = torrent

    def receive_message(self):

        """ 
        Receive bitfield message from peer after handshake which tells us which pieces peer has
        """

        buffer = bytearray()
        try:
            # Keep reading until you get first 4 bytes = message length prefix 
            while len(buffer) < 4:
                chunk = self.socket.recv(4096)
                if not chunk:
                    return None
                buffer += chunk  

            mssg_length = int.from_bytes(buffer[:4], 'big')

            # Keep reading until we have the entire message
            while len(buffer) < mssg_length + 4:
                chunk = self.socket.recv(4096)

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
        
        except socket.timeout:
            logger.debug("Socket timed out in receive message function")
        except socket.error as e:
            logger.debug(f"Error in receive message: {e}")    
    
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
    
    def decide_interest(self):

        """
        Check if peer has pieces we don't have and then send interested message and wait for unchoke message
        """

        pieces_needed = self.pieces_peer_has - self.piece_manager.have_pieces

        if pieces_needed:
            try:
                self.socket.sendall(self.get_interested_message())  # type: ignore

            except socket.timeout:
                logger.debug("Timed out sending interested messsage")
                self.socket.close()
                return False
            
            except socket.error as e:
                logger.debug(f"Error sending interest to: {e} to {self.ip}")
                self.socket.close()
                return False

            try: 
                response = self.socket.recv(4096)
                
            except socket.timeout:
                logger.debug("Timed out receiving response")
                self.socket.close()
                return False 
            
            except socket.error as e:
                logger.debug(f"Error receiving response: {e} to {self.ip}")
                self.socket.close()
                return False
            
            if len(response) >= 5 and response[4] == 1:
                logger.debug("Peer has unchoked you.")
                self.interested = True
                self.peer_choking = False
                return True 
            
        logger.debug("This peer has no pieces we need")
        return False
    
    def request_block(self, piece_index, offset, block_size):

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
            self.socket.sendall(request_mssg)
            logger.debug(f"Block requested from {self.ip}")
        except socket.timeout:
            logger.debug("Timed out requesting piece")
        except socket.error as e:
            logger.debug(f"Error while requesting piece {e}") 
    
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

    
    def get_all_pieces(self):

        """
        Handles the looping of pieces and blocks in the file(s), verifies hash of each completed piece and stores it
        """

        block_size = 16384
        starting_piece_count = len(self.piece_manager.have_pieces)
        common = self.pieces_peer_has & self.piece_manager.get_missing_pieces()
        incomplete_piece = False
        pieces_peer_not_have = 3

        for piece_index in sorted(common):

            if self.piece_manager.is_piece_complete(piece_index): 
                continue   

            if pieces_peer_not_have == 0:
                break

            is_last_piece = piece_index == self.total_pieces - 1
            current_piece_length = self.last_piece_length \
                if is_last_piece else self.piece_length

            self.requested_pieces.add(piece_index)
            total_blocks = math.ceil(current_piece_length / block_size)
            self.requested_blocks[piece_index] = {}

            for i in range(total_blocks):
                offset = i * block_size

                current_block = min(block_size, current_piece_length - offset)
                self.request_block(piece_index, offset, current_block)   

                call = 3
                recieved_block = False
                
                while call > 0 and not recieved_block:
                    response = self.receive_message()  

                    if response is None:
                        logger.debug(f"No response for block {i} of piece {piece_index} from {self.ip}")
                        call -= 1  
                        continue 
                        
                    if response["id"] == 7:
                        self.get_piece_message(response["content"], i)
                        if piece_index in self.requested_blocks and offset in self.requested_blocks[piece_index]:
                            recieved_block = True
                    else:
                        logger.debug("Incorrect message id from peer during block receiving")

                if not recieved_block:
                    logger.debug(f"Block {i} of piece {piece_index} could not be downloaded")
                    logger.debug(f"Skipping piece {piece_index}\n")
                    incomplete_piece = True
                    break

            if incomplete_piece:
                pieces_peer_not_have -= 1
                continue    

            got_blocks = self.requested_blocks[piece_index]

            if len(got_blocks) != total_blocks:
                logger.debug(f"Piece {piece_index} incomplete,")
                continue

            full_piece_data = bytearray()
            for j in range(total_blocks):
                offset = j * block_size
                full_piece_data += got_blocks[offset]

            if self.verify_piece(piece_index, full_piece_data): # type: ignore
                self.piece_manager.piece_complete(piece_index, full_piece_data)
                logger.debug(f"Completed piece {piece_index}\n") 
            else:
                logger.debug(f"Piece {piece_index} not added as hash does not match")    
               

        completed_from_this_peer = len(self.piece_manager.have_pieces) - starting_piece_count       
        
        if self.piece_manager.is_download_complete():
            return True
        else:
            logger.debug(f"Finished downloading {completed_from_this_peer} from this peer")
            logger.debug(f"Total pieces downloaded: {len(self.piece_manager.have_pieces)} / {self.piece_manager.total_pieces}")
            return False

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


            