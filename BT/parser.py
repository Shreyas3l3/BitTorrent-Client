import bencodepy # type: ignore
import hashlib

class TorrentDecoder:
    def __init__(self, filepath):
        self.filepath = filepath
        self.metadata = self.load()
        self.multi_file = False
        self.http = True

    def load(self):
        with open(self.filepath, "rb") as f:
            return bencodepy.decode(f.read())
        
    @staticmethod    
    def decode_bytes(item):
        if isinstance(item, dict):
            new_dict = {}
            for key, value in item.items():
                new_key = TorrentDecoder.decode_bytes(key)
                new_value = TorrentDecoder.decode_bytes(value)
                new_dict[new_key] = new_value
            return new_dict
        
        elif isinstance(item, bytes):
            try:
                return item.decode('utf-8')
            except UnicodeDecodeError:
                return item.hex()
        else:
            return item        
            
    def get_info_dict(self):
        info = self.metadata[b'info'] 
        return TorrentDecoder.decode_bytes(info)  
    
    def get_piece_length(self):
        return int(self.metadata[b'info'][b'piece length'])
    
    def get_file_length(self):
        info = self.metadata[b'info'] 
        if b'length' in info:
            return int(info[b'length'])
        else:
            multi_file_length = 0
            for file in info[b'files']:
                multi_file_length += int(file[b'length'])
                self.multi_file = True
            return multi_file_length   

    def get_file_list(self):
        info = self.metadata[b'info']
        file_list = []

        if b'files' in info:
            for file in info[b'files']:
                part_path = [self.decode_bytes(_) for _ in file[b'path']]
                path = "/".join(part_path)
                file_list.append({
                    "Path": path,
                    "Length": int(file[b'length']) 
                })
        else:
            file_list.append({
                "Path": self.decode_bytes(info[b'name']),
                "Length": int(info[b'length'])
            })  

        return file_list         

    def get_piece_hashes(self):
        all_pieces = []
        pieces = self.metadata[b'info'][b'pieces']
        for i in range(0, len(pieces), 20):
            new_piece = pieces[i: i + 20]
            all_pieces.append(new_piece)
        return all_pieces    
    
    def get_announce(self):
        announce = self.metadata.get(b'announce').decode('utf-8')
        if announce[0] == "u":
            self.http = False
        return announce
    
    def get_info_hash(self):
        encoded_info = bencodepy.encode(self.metadata[b'info'])
        return hashlib.sha1(encoded_info).digest()
    
    def get_number_of_pieces(self):
        pieces_field = self.metadata[b'info'][b'pieces']
        return len(pieces_field) // 20
    
    def get_last_piece_length(self):
        total_length = self.get_file_length()
        piece_length = self.get_piece_length()
        left = total_length % piece_length
        if left != 0:
            return left
        return piece_length

    def get_metadata(self, port, peer_id):
        return {
            "announce url": self.get_announce(),
            "info_hash": self.get_info_hash(),
            "file length": self.get_file_length(),
            "peer_id": peer_id,
            "port": port
        }
    
    def get_file_name(self):
        info = self.metadata[b'info']
        return self.decode_bytes(info[b'name'])
    
    def is_torrent_multi_file(self):
        return self.multi_file
    