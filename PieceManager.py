import os
import logging
import asyncio

logger = logging.getLogger(__name__)

class PieceManager:
    def __init__(self, total_pieces, torrent):
        self.have_pieces = set()
        self.total_pieces = total_pieces
        self.pieces_data = {}
        self.downloading_pieces = set()
        self.torrent = torrent
        self.lock = asyncio.Lock()

    async def piece_complete(self, piece_index, piece_data):

        """
        Adds piece data to dict containing data of all currently downloaded pieces
        """

        async with self.lock:
            self.have_pieces.add(piece_index)
            self.downloading_pieces.discard(piece_index)
            self.pieces_data[piece_index] = piece_data

    async def is_piece_complete(self, piece_index):

        """
        Returns boolean whether piece has already been downloaded
        """
        async with self.lock:
            return piece_index in self.have_pieces

    async def is_download_complete(self):

        """
        Checks if we have all pieces 
        """
        async with self.lock:
            for piece in list(self.downloading_pieces):
                if piece in self.have_pieces:
                    self.downloading_pieces.discard(piece)
            logger.debug(f"{len(self.have_pieces)} and {self.total_pieces}")
            return len(self.have_pieces) == self.total_pieces   
    
    async def get_missing_pieces(self):

        """
        Returns a set of pieces we are missing
        """

        async with self.lock:
            for piece in list(self.downloading_pieces):
                if piece in self.have_pieces:
                    self.downloading_pieces.discard(piece)
            return set(range(self.total_pieces)) - self.have_pieces - self.downloading_pieces

    async def is_piece_downloading(self, piece_index):

        """
        Returns a boolean whether a piece is currently downloading or already downloaded
        """

        async with self.lock:
            if piece_index in self.downloading_pieces or piece_index in self.have_pieces:
                return True
            self.downloading_pieces.add(piece_index)
            return False    
        
    async def piece_failed(self, piece_index):

        """
        Acknowledge a piece as failed by removing it from downloading pieces
        """

        async with self.lock:
            self.downloading_pieces.discard(piece_index)
            logger.debug(f"Piece {piece_index} marked as failed, removed from downloading set")

    async def get_info(self):
            
            """
            Get info about piece download process
            """

            async with self.lock:
                return {
                    'have': len(self.have_pieces),
                    'downloading': len(self.downloading_pieces),
                    'total': self.total_pieces,
                    'downloading_pieces': sorted(self.downloading_pieces),
                    'missing': self.total_pieces - len(self.have_pieces) - len(self.downloading_pieces)
                }
            
            
    def write_to_file(self):

        """
        For multi-file torrents, this function puts all piece data into bytearray and splits data exactly so it matches each file size,
        and creates a folder for all files
        For single file torrents, we just write all data to file
        """

        all_data = bytearray()
        for data in sorted(self.pieces_data):
                all_data.extend(self.pieces_data[data])      

        if self.torrent.is_torrent_multi_file():
            file_list = self.torrent.get_file_list()
            offset = 0

            for i in range(len(file_list)):
                file_name = file_list[i]["Path"]
                file_length = file_list[i]["Length"]

                file_bytes = all_data[offset:offset + file_length]
                offset += file_length

                if file_bytes:
                    folder = os.path.dirname(file_name)
                    if folder:
                        os.makedirs(folder, exist_ok=True)
                    with open(file_name, 'wb') as f:
                        f.write(file_bytes)
                        logger.info(f"File {i}: {file_name} downloaded!")

                else:        
                    logger.info(f"File {i}: {file_name} could not be downloaded") 

        else: 
            file_name = self.torrent.get_file_name()
            with open(file_name, 'wb') as f:
                    f.write(all_data)
                    logger.info(f"File: {file_name} downloaded!")

