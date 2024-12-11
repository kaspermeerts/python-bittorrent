import socket
import struct
import urllib.parse
import urllib.request
from bencode import bdecode


def __decode_peers_dictionary(peerlist):
    try:
        return [(peer[b"ip"].decode("ascii"), peer[b"port"]) for peer in peerlist]
                 # peer.get(b"peer id"))  FIXME, add peer id back in
    except KeyError as e:
        raise ValueError("missing key in tracker response: %s" % e.args[0])
    except UnicodeDecodeError as e:
        raise ValueError("invalid peer host name") from e


def __decode_peers_binary(peerlist):
    if len(peerlist) % 6 != 0:
        raise ValueError("length of binary peer list should be"
                         "a multiple of 6, is %d" % len(peerlist))

    peers = []
    for i in range(0, len(peerlist), 6):
        addr = socket.inet_ntoa(peerlist[i:i+4])
        port = struct.unpack("!H", peerlist[i+4:i+6])[0]
        peers.append((addr, port)) # FIXME add peer id back in

    return peers


def decode_peers(peerlist):
    if type(peerlist) is list:
        return __decode_peers_dictionary(peerlist)
    elif type(peerlist) is bytes:
        return __decode_peers_binary(peerlist)
    else:
        raise TypeError("invalid type for peer list: %s" % type(peerlist))


class Tracker:
    def __init__(self, announce_url):
        self.announce_url = announce_url

        res = urllib.parse.urlparse(self.announce_url)
        if res.scheme != "http":
            raise ValueError("only http trackers supported")

        self.scrape_url = None
        slash = self.announce_url.rfind("/")
        if slash != -1 and self.announce_url.startswith("/announce", slash):
            self.scrape_url = self.announce_url.replace("/announce", "/scrape")

        self.tracker_id = None
        self.interval = None
        self.scrape_interval = None

    def announce(self, torrent, peer_id, event=None, numwant=5):
        if event and event not in ("started", "stopped", "completed"):
            raise ValueError("event must be one of"
                             "started, stopped or completed")

        values = {
            "info_hash": bytes.fromhex(torrent.info_hash),
            "peer_id": peer_id,
            "port": 6880, # FIXME
            "uploaded": torrent.get_uploaded(),
            "downloaded": torrent.get_downloaded(),
            "left": torrent.file.filesize,
            "compact": 1,
            "numwant": numwant
            }
        if event:
            values["event"] = event
        if self.tracker_id:
            values["trackerid"] = self.tracker_id

        data = urllib.parse.urlencode(values)
        url = urllib.request.Request(self.announce_url + '?' + data)
        try:
            with urllib.request.urlopen(url) as request:
                if request.getcode() != 200:
                    raise RuntimeError("failure to announce")
                response = bdecode(request.read())
        except urllib.error.URLError as e:
            raise RuntimeError("failure to announce") from e
        except ValueError as e:
            raise RuntimeError("invalid tracker response") from e

        if b"failure reason" in response:
            raise RuntimeError("Failure to announce: %s" % response[b"failure_reason"])

        seeders = leechers = None
        if b"complete" in response and b"incomplete" in response:
            seeders = response[b"complete"]
            leechers = response[b"incomplete"]

        if b"tracker id" in response:
            self.tracker_id = response[b"tracker id"]

        try:
            peers = decode_peers(response[b"peers"])
        except (KeyError, ValueError) as e:
            raise RuntimeError("invalid tracker response") from e

        return {"seeders": seeders, "leechers": leechers, "peers": peers}

    def scrape(self, info_hash):
        if self.scrape_url is None:
            raise RuntimeError("tracker doesn't support scrape")

        values = {"info_hash": bytes.fromhex(info_hash)}
        data = urllib.parse.urlencode(values)
        url = urllib.request.Request(self.scrape_url + '?' + data)
        try:
            with urllib.request.urlopen(url) as request:
                if request.getcode() != 200:
                    raise RuntimeError("failure to scrape")
                response = bdecode(request.read())
        except urllib.error.URLError:
            raise RuntimeError("failure to scrape")
        except ValueError as e:
            raise RuntimeError("invalid tracker response") from e

        assert b"files" in response
        try:
            info = response[b"files"][bytes.fromhex(info_hash)]
            ret = {"seeders": info[b"complete"],
                   "leechers": info[b"incomplete"]}
        except KeyError:
            raise RuntimeError("invalid tracker response")

        if b"flags" in response:
            self.scrape_interval = response[b"flags"].get(b"min_request_interval", None)

        return ret
