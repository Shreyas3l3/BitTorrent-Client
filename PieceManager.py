import os
import logging

logger = logging.getLogger(__name__)

class PieceManager:
    def __init__(self, total_pieces, torrent):
        self.have_pieces = set()
        self.total_pieces = total_pieces
        self.pieces_data = {}
        self.torrent = torrent

    def piece_complete(self, piece_index, piece_data):

        """
        Adds piece data to dict
        """
        self.have_pieces.add(piece_index)
        self.pieces_data[piece_index] = piece_data

    def is_piece_complete(self, piece_index):

        """
        Returns boolean whether piece has already beenn downloaded
        """
        return piece_index in self.have_pieces

    def is_download_complete(self):

        """
        Checks if we have all pieces 
        """
        return len(self.have_pieces) == self.total_pieces   

    def get_piece_data(self, piece_index):

        """
        Returns data of specified piece
        """
        return self.pieces_data.get(piece_index, None)  

    def get_all_piece_data(self):

        """
        Return data of all pieces
        """
        return self.pieces_data

    def get_missing_pieces(self):

        """
        Returns a list of pieces we are missing
        """

        missing_pieces = set()
        for i in range(self.total_pieces):
            if i not in self.pieces_data:
                missing_pieces.add(i)        
        return missing_pieces         
    
    def write_to_file(self):

        """
        For multi-file torrents, this function puts all piece data into bytearray and splits data exactly so it macthes each file size,
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

    