from collections import namedtuple
from enum import Enum
import struct
from bitarray import bitarray

# TODO TransferHistory class, containing the bytes transferred the last n
# seconds, to estimate the recent transfer speed. Keep total transferred bytes

Request = namedtuple("Request", "index begin length")


# TODO Add new states WAIT_BLOCK and DONE_BLOCK, and use socket.sendfile
# to store it immediately without copying the buffers
class MessageProducer:
    class States(Enum):
        WAIT_LENGTH, WAIT_ID, WAIT_PAYLOAD, DONE = range(4)

    def __init__(self):
        self.state = MessageProducer.States.WAIT_LENGTH
        self._length_buffer = b""
        self.length = None
        self.msg_id = None
        self.payload = b""

    def _next(self):
        if self.state == MessageProducer.States.WAIT_LENGTH:
            if self.length == 0:
                self.state = MessageProducer.States.DONE
            else:
                self.state = MessageProducer.States.WAIT_ID
        elif self.state == MessageProducer.States.WAIT_ID:
            if self.length == 1:
                self.state = MessageProducer.States.DONE
            else:
                self.state = MessageProducer.States.WAIT_PAYLOAD
        elif self.state == MessageProducer.States.WAIT_PAYLOAD:
            self.state = MessageProducer.States.DONE
        elif self.state == MessageProducer.States.DONE:
            pass

    def _receive_length(self, buffer):
        bytes_needed = 4 - len(self._length_buffer)
        self._length_buffer += buffer[:bytes_needed]

        if len(self._length_buffer) == 4:
            (self.length,) = struct.unpack("!I", self._length_buffer)
            self._next()

        return buffer[bytes_needed:]

    def _receive_id(self, buffer):
        (self.msg_id,) = struct.unpack("!B", buffer[:1])
        self._next()

        return buffer[1:]

    def _receive_payload(self, buffer):
        bytes_needed = self.length - (1 + len(self.payload))
        self.payload += buffer[:bytes_needed]

        if 1 + len(self.payload) == self.length:
            self._next()

        return buffer[bytes_needed:]

    def consume(self, buffer):
        if self.state == MessageProducer.States.WAIT_LENGTH:
            buffer = self._receive_length(buffer)
        elif self.state == MessageProducer.States.WAIT_ID:
            buffer = self._receive_id(buffer)
        elif self.state == MessageProducer.States.WAIT_PAYLOAD:
            buffer = self._receive_payload(buffer)
        elif self.state == MessageProducer.States.DONE:
            pass

        return buffer

    def reset(self):
        self.__init__()


class Peer:
    def __init__(self, socket, address, peer_id, file):
        # TODO Split this class in PeerState and PeerInfo, and merge with
        # MessageProducer? A lot of these attributes are only manipulated
        # from the outside. And Bittorrent messages can only change the state
        self.socket = socket
        self.address = address
        self.peer_id = peer_id
        self.file = file  # FIXME Remove dependency
        self.downloaded = 0
        self.uploaded = 0
        self.message_producer = MessageProducer()
        self.write_buffer = b""
        self.dead = False

        # TODO Abstract out this peer state ?
        self.state = {}
        self.state["is_choking"] = True
        self.state["is_interested"] = False
        self.state["am_choking"] = True
        self.state["am_interested"] = False
        self.state["has_pieces"] = bitarray(self.file.num_pieces * '0',
                                            endian="big")
        self.state["has_pieces"].fill()
        self.state["in_requests"] = []
        self.state["out_requests"] = []
        self.state["completed_requests"] = []

    def __repr__(self):
        flags = ""
        flags += "c" if self.state["is_choking"]    else "u"
        flags += "i" if self.state["is_interested"] else "d"
        flags += "C" if self.state["am_choking"]    else "U"
        flags += "I" if self.state["am_interested"] else "D"

        return "<Peer %s, %s, %d/%d, Down: %d, Up: %d>" % (
            self.address, flags, sum(self.state["has_pieces"]),
            self.file.num_pieces, self.downloaded, self.uploaded)

    def _handle_request(self, payload):
        (index, begin, length) = struct.unpack("!III", payload)
        if not self.file.pieces[index].verified:
            print("peer asked for piece we don't have")
            self.dead = True
            return
        if self.state["am_choking"]:
            print("request from choked peer")
            self.dead = True
            return
        if len(self.state["in_requests"]) > 512:  # FIXME
            print("peer sending too much requests")
            self.dead = True  # TODO Too strict?
            return
        request = Request(index, begin, length)
        print("Request message %d:%d:%d" % request)
        self.state["in_requests"].append(request)

    def _handle_block(self, payload):
        (index, begin) = struct.unpack("!II", payload[:8])
        block = payload[8:]
        length = len(block)
        request = Request(index, begin, length)
        print("Incoming block data! %d:%d:%d" % request)
        if request not in self.state["out_requests"]:
            print("I didn't ask for this...")
            self.dead = True
            return
        self.state["out_requests"].remove(request)
        if self.file.pieces[index].verified:
            print("We asked for it, but the piece is already verified")
            # self.dead = True
            return  # FIXME this could happen...
        self.file.store_block(index, begin, block)
        self.state["completed_requests"].append(request)

    def _handle_cancel(self, payload):
        index, begin, length = struct.unpack("!III", payload)
        request = Request(index, begin, length)
        print("Cancel message %d:%d:%d" % request)
        if request not in self.state["in_requests"]:
            print("Peer canceled block it never requested...")
            self.dead = True
            return
        self.state["in_requests"].remove(request)

    def _handle_message(self, length, msg_id, payload):
        if length == 0:
            print("Received keepalive")
            return

        assert length == 1 + len(payload)

        if msg_id == 0:
            print("Choked! 🔇")
            self.state["is_choking"] = True
        elif msg_id == 1:
            print("Unchoked! 🔊")
            self.state["is_choking"] = False
        elif msg_id == 2:
            print("Interested ❤")
            self.state["is_interested"] = True
        elif msg_id == 3:
            print("Not interested 💔")
            self.state["is_interested"] = False
        elif msg_id == 4:
            print("He has a piece 🔫")
            (index,) = struct.unpack("!I", payload)
            if index >= self.file.num_pieces:
                print("peer sent out-of-bounds piece index: %d" % index)
                self.dead = True
                return
            self.state["has_pieces"][index] = True
        elif msg_id == 5:
            print("In Flanders bitfields")
            self.state["has_pieces"] = bitarray(endian="big")
            self.state["has_pieces"].frombytes(payload)
        elif msg_id == 6:
            self._handle_request(payload)
        elif msg_id == 7:
            self._handle_block(payload)
        elif msg_id == 8:
            self._handle_cancel(payload)
        else:
            print("peer sent message with unknown id: %d" % msg_id)
            self.dead = True
            return

    def _check_length(self, length, msg_id):
        if msg_id in (0, 1, 2, 3):  # (un)choke, (dis)interested
            return length == 1
        elif msg_id == 4:  # have
            return length == 1 + 4
        elif msg_id == 5:  # bitfield
            return length == 1 + (len(self.state["has_pieces"]) + 7) // 8
        elif msg_id in (6, 8):  # request, cancel
            return length == 1 + 4 + 4 + 4
        elif msg_id == 7:  # block
            return length > 1 + 4 + 4 and length <= 1 + 4 + 4 + BLOCKSIZE
        else:
            return False

    def read_messages(self, buffer):
        while buffer:
            # TODO increment offset in buffer instead of doing partial copies
            buffer = self.message_producer.consume(buffer)

            if self.message_producer.state == MessageProducer.States.WAIT_PAYLOAD:
                if not self._check_length(self.message_producer.length,
                                          self.message_producer.msg_id):
                    print("Peer sent message with invalid length")
                    self.dead = True
                    return


            if self.message_producer.state == MessageProducer.States.DONE:
                self._handle_message(self.message_producer.length,
                                     self.message_producer.msg_id,
                                     self.message_producer.payload)
                self.message_producer.reset()

    def _send(self, msg_id=None, payload=b""):
        if msg_id is None:
            msg = struct.pack("!I", 0)
        else:
            length = 1 + len(payload)
            msg = struct.pack("!IB", length, msg_id) + payload
            assert self._check_length(msg_id, length)

        self.write_buffer += msg

    def send_keepalive(self):
        print("Sending keepalive")
        self._send()

    def choke(self):
        if self.state["am_choking"]:
            return
        print("Sending choke")
        self._send(0)
        self.state["am_choking"] = True

    def unchoke(self):
        if not self.state["am_choking"]:
            return
        print("Sending unchoke")
        self._send(1)
        self.state["am_choking"] = False

    def interested(self):
        if self.state["am_interested"]:
            return
        print("Sending interested")
        self._send(2)
        self.state["am_interested"] = True

    def not_interested(self):
        if not self.state["am_interested"]:
            return
        print("Sending not interested")
        self._send(3)
        self.state["am_interested"] = False

    def send_have(self, index):
        assert self.file.pieces[index].verified
        print("Sending have")
        self._send(4, struct.pack("!I", index))

    def send_bitfield(self):
        print("Sending bitfield")
        payload = self.file.get_bitfield().tobytes()
        self._send(5, payload)

    def request(self, request):
        print("Sending request")
        self.state["out_requests"].append(request)
        index, begin, length = request
        self._send(6, struct.pack("!III", index, begin, length))

    # TODO use socket.sendfile
    def send_block(self, request):
        index, begin, length = request
        assert self.file.pieces[index].verified
        block = self.file.read_block(index, begin, length)
        print("Sending block")
        self._send(7, struct.pack("!II", index, begin) + block)

    def send_cancel(self, request):
        index, begin, length = request
        print("Sending cancel")
        self._send(8, struct.pack("!III", index, begin, length))
