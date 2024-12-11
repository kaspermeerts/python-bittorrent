import hashlib
import random
import selectors
import socket as Socket
import struct
from bencode import bdecode, bencode
from peer import Peer, Request
from tracker import Tracker
from file import File, BLOCKSIZE

BUFFER_SIZE = 4096

ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
PEER_ID = str.encode("-PY0000-" +
                     "".join([random.choice(ALPHABET) for _ in range(12)]))
assert len(PEER_ID) == 20

# TODO put in some sort of utils module?
def random_set_bit(bitfield):
    nset = bitfield.count()
    nrandom = random.randrange(nset)

    index = bitfield.index(True)
    while nrandom > 0:
        index = bitfield.index(True, index + 1)
        nrandom -= 1

    return index

#TODO PeerMgr class?
def send_handshake(socket, info_hash, peer_id):
    handshake = struct.pack("!B19s8s20s20s", 19, b"BitTorrent protocol",
                            8*b'\0', bytes.fromhex(info_hash), peer_id)

    socket.send(handshake)

def recv_handshake(socket, info_hash):
    response = socket.recv(1 + 19 + 8 + 20 + 20)
    try:
        response = struct.unpack("!B19s8s20s20s", response)
    except struct.error as e:
        raise RuntimeError("invalid response") from e

    if response[0] != 19 or response[1] != b"BitTorrent protocol":
        raise RuntimeError("unknown protocol: %s" % response[1])

    if response[3] != bytes.fromhex(info_hash):
        raise RuntimeError("wrong info hash: %s" % hex(response[3]))

    return response[4]

class Torrent:
    def __init__(self, filename):
        self.info_hash = None
        self.trackers = []
        self.swarm = set()
        self.peers = set()
        self._downloaded = 0
        self._uploaded = 0
        self.file = None

        with open(filename, "rb") as tr_file:
            contents = tr_file.read()
        try:
            self.metainfo = bdecode(contents)  # Do we need to keep this?
            self.parse_metainfo(self.metainfo)
        except (ValueError, KeyError, UnicodeError) as e:
            raise ValueError("invalid or corrupt torrent file") from e

    def parse_metainfo(self, metainfo):
        sha = hashlib.sha1(bencode(metainfo[b"info"]))
        self.info_hash = sha.hexdigest()

        if b"announce-list" in metainfo:
            for tier in metainfo[b"announce-list"]:
                for url in tier:
                    tracker = Tracker(url.decode())
                    self.trackers.append(tracker)
        elif b"announce" in metainfo:
            url = self.metainfo[b"announce"].decode()
            self.trackers = [Tracker(url)]
        else:
            pass
            #raise ValueError("no tracker in torrent file")

        info = self.metainfo[b"info"]
        self.file = File(info[b"name"], info[b"length"],
                         info[b"piece length"], info[b"pieces"])

    def get_downloaded(self):
        return self._downloaded + sum([p.downloaded for p in self.peers])

    def get_uploaded(self):
        return self._uploaded + sum([p.uploaded for p in self.peers])

    def connect(self, address, expected_peer_id=None):
        socket = Socket.create_connection(address, timeout=5) # FIXME...

        send_handshake(socket, self.info_hash, PEER_ID)
        recv_peer_id = recv_handshake(socket, self.info_hash)

        if expected_peer_id and expected_peer_id != recv_peer_id:
            socket.close()
            raise RuntimeError("peer id %s from peer %r doesn't match"
                               "what we got from the tracker: %s" %
                               (recv_peer_id, address, expected_peer_id))

        new_peer = Peer(socket, address, recv_peer_id, self.file)
        self.peers.add(new_peer)
        return new_peer

    def mainloop(self):
#        ret = self.trackers[0].announce(self, PEER_ID, event="started",numwant=20)
#        self.swarm |= set(ret["peers"])
        self.swarm = {("127.0.0.1", 58427)}

        selector = selectors.DefaultSelector()

        # connect to some peers until we're at 10 or so FIXME
        for addr in random.sample(self.swarm, 1):
            try:
                peer = self.connect(addr)
                peer.send_bitfield()
                selector.register(peer.socket, selectors.EVENT_READ, peer)
            except OSError as e:
                print(e)
                print("Couldn't connect to peer... %r" % (addr,))

        counter = 1

        while True:
            for peer in self.peers:
                if peer.dead:
                    print("Deleting peer %r" % peer)
                    selector.unregister(peer.socket)
                    peer.socket.shutdown()
                    peer.socket.close()
                    self.peers.remove(peer)
                    self._downloaded += peer.downloaded
                    self._uploaded += peer.uploaded
                elif peer.write_buffer:
                    selector.modify(peer.socket, selectors.EVENT_READ |
                                                 selectors.EVENT_WRITE, peer)
                else:
                    selector.modify(peer.socket, selectors.EVENT_READ, peer)

            events = selector.select(2) # FIXME LOL

            for key, mask in events:
                peer = key.data

                if mask & selectors.EVENT_READ:
                    buffer = peer.socket.recv(BUFFER_SIZE)
                    if len(buffer) == 0:
                        peer.dead = True
                    peer.downloaded += len(buffer)
                    peer.read_messages(buffer)

                if mask & selectors.EVENT_WRITE:
                    sent = peer.socket.send(peer.write_buffer)
                    if sent == 0:
                        peer.dead = True
                    peer.uploaded += sent
                    peer.write_buffer = peer.write_buffer[sent:]

            # send have message for every newly completed piece
            new_haves = set()
            for peer in self.peers:
                while peer.state["completed_requests"]:
                    (index, _, _) = peer.state["completed_requests"].pop()
                    if self.file.pieces[index].verified:
                        new_haves.add(index)

            for peer in self.peers:
                for index in new_haves:
                    peer.send_have(index)
                    peer.send_bitfield()

            our_pieces = self.file.get_bitfield()
            if self.file.num_pieces == our_pieces.count():
                print("Were done!!")
                break

            for peer in self.peers:
                his_pieces = peer.state["has_pieces"]
                want_pieces = his_pieces & ~our_pieces
                if not any(want_pieces):
                    continue # they have nothing we want

                # tell them we're interested
                peer.interested()

                # wait till unchoke
                if peer.state["is_choking"]:
                    continue

                # send requests for rarest pepes
                if len(peer.state["out_requests"]) > 20: # FIXME :p
                    continue

                ctr = 0
                request = None # FIXME...
                while request is None or request in peer.state["out_requests"] and ctr < 20:
                    piece_idx = random_set_bit(want_pieces)

                    blocks_we_dont_have = ~self.file.pieces[piece_idx].block_progress
                    block_idx = random_set_bit(blocks_we_dont_have)

                    length = self.file.pieces[piece_idx].get_block_length(block_idx)
                    request = Request(piece_idx, block_idx*BLOCKSIZE, length)
                    ctr += 1

                print("Request %d: %r" % (counter, request))
                peer.request(request)
                counter += 1

            # eat sleep rave repeat


if __name__ == "__main__":
    arch = Torrent("arch.torrent")
    pic = Torrent("pic.torrent")
    ubuntu = Torrent("ubuntu.torrent")
    payload = Torrent("payload.torrent")
    # fedora = Torrent("fedora.torrent")
    # d = Torrent("/tmp/debian.torrent")
    h1 = "50bceef91cb7ce8933274b6e6016454581f77a19"
    h2 = "8baffd307f47f506769a3e136fc5921e90bde158"
    h3 = "382b3f154c9c8d478f87d30d625a8989f420b965"
    f = File("kasper.txt", 24, 9, bytes.fromhex(h1 + h2 + h3))
