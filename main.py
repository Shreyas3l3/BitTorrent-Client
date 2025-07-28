import sys
import logging

log_level = logging.DEBUG if len(sys.argv) == 3 and sys.argv[2].lower() == "debug" else logging.INFO

logging.basicConfig(
    level=log_level,
    format='%(message)s'
)

logger = logging.getLogger(__name__)

from parser import TorrentDecoder
from tracker import Tracker
import random
import string
from handshake import Handshake # type: ignore
from exchange import exchange
from PieceManager import PieceManager

def main():

    def generate_peer_id():
        id = "-SB001-"
        characters = string.ascii_lowercase + string.digits
        result = "".join(random.choices(characters, k=13))
        return id + result

    if len(sys.argv) > 3:
        logger.info("Usage: python3 main.py <torrent file path> <optional: debug>")
        sys.exit()
    else:    
        torrent_path = sys.argv[1]
        torrent = TorrentDecoder(torrent_path)
     

    port = 6885 
    peer_id = generate_peer_id() 
    
    metadata = torrent.get_metadata(port, peer_id) # type: ignore
    piece_length = torrent.get_piece_length()
    last_piece_length = torrent.get_last_piece_length()
    file_list = torrent.get_file_list()
    http = torrent.http
    
    
    for i in range(len(file_list)):
        logger.info(f"File name:  {file_list[i]["Path"]} ||  File size:  {file_list[i]["Length"]}")


    logger.debug("\n===================")
    logger.debug(f"Length of each piece: {piece_length:,}")
    logger.debug(f"Total File length: {torrent.get_file_length():,}")
    total_pieces = torrent.get_number_of_pieces()
    logger.debug(f"Number of pieces: {total_pieces}")
    logger.debug("===================\n")

    logger.debug(f"Announce: {torrent.get_announce()}")

    tracker = Tracker(
        announce_url = metadata["announce url"],
        info_hash = metadata["info_hash"],
        peer_id = metadata["peer_id"],
        port = metadata["port"],
        file_length = metadata["file length"],
        http = http
    )
    
    if http:
        peer_list = tracker.decode_peer_list(tracker.send_request())
    else:
        response = tracker.send_connect_request()
        if response is None:
            sys.exit()
        else:
            peer_list = tracker.decode_peer_list(response)    
           
    logger.debug("\nAvailable peers: \n")
    logger.debug(f"{peer_list}\n")
    
    piece_manager = PieceManager(total_pieces=total_pieces, torrent=torrent) 
    logger.info("Gathering pieces from peers...")

    def download_from_peers(peer_list, metadata, peer_id, piece_length, total_pieces,
                            last_piece_length, piece_manager, torrent):
        
        max_attempts = 3
        attempt = 0
        skip_these_peers = set()

        while not piece_manager.is_download_complete() and attempt < max_attempts: 
            attempt += 1 

            if attempt != 1:
                logger.debug("Attempting to download missing pieces/n")

            for ip, port in peer_list:

                if (ip, port) in skip_these_peers:
                    continue
                
                handshake = Handshake(
                ip = ip,
                port = port,
                info_hash = metadata["info_hash"],
                peer_id = peer_id
                )

                if handshake.connect_with_peer():

                    ex = exchange(
                        socket = handshake.socket,
                        info_hash = handshake.info_hash,
                        peer_id = handshake.peer_id,
                        ip = ip,
                        piece_length = piece_length,
                        total_pieces = total_pieces,
                        last_piece_length = last_piece_length,
                        piece_manager = piece_manager,
                        torrent = torrent
                    ) 

                    bitfield = ex.receive_message()

                    if bitfield and bitfield["id"] == 5:
                        ex.parse_message(bitfield["content"], torrent.get_number_of_pieces())
                    else:
                        logger.debug(f"Did not recieve valid bitfield from {ip}") 
                        skip_these_peers.add((ip, port))   
                        continue
           
                    if ex.decide_interest():
                        had_pieces = len(piece_manager.have_pieces)
                        try:
                            finished = ex.get_all_pieces()
                        except Exception as e:
                            logger.debug(f"Error downloading from {ip}: {e}")
                            skip_these_peers.add((ip, port))   
                            continue 

                        if finished:
                            logger.info("All pieces downloaded")
                            return

                        new_pieces = len(piece_manager.have_pieces) - had_pieces

                        if new_pieces == 0:
                            skip_these_peers.add((ip, port))

                    else:
                        logger.debug(f"Peer did not unchoke {ip}") 
                        skip_these_peers.add((ip, port))

        if not piece_manager.is_download_complete():
            logger.info("All pieces could not be download, please retry downloading this torrent.") 
               
                          
    download_from_peers(peer_list, metadata, peer_id, piece_length, total_pieces,
                            last_piece_length, piece_manager, torrent)
    
    logger.info("Attempting to download file(s) to disk now...")
    piece_manager.write_to_file()  

if __name__ == "__main__":
    main()
     

    
        





      
     

    
   
    
    
   