from collections import OrderedDict


def __decode_int(bytestring, start):
    end = bytestring.index(b'e', start)
    if bytestring[start] == ord('0') and end != start + 1 or \
       bytestring[start] == ord('-') and bytestring[start + 1] == ord('0'):
        raise ValueError("invalided bencoded integer at position %d" % start)
    result = int(bytestring[start:end])
    return result, end + 1

def __decode_string(bytestring, start):
    colon = bytestring.index(b':', start)
    str_len = int(bytestring[start:colon].decode("ascii"))
    if colon + 1 + str_len > len(bytestring):
        raise ValueError("truncated string at position %d" % start)
    result = bytestring[colon + 1 : colon + 1 + str_len]
    return result, colon + 1 + str_len

def __bdecode(bytestring, start):
    if bytestring[start] == ord('i'):
        (result, new_start) = __decode_int(bytestring, start + 1)
    elif bytestring[start] in b"0123456789":
        (result, new_start) = __decode_string(bytestring, start)
    elif bytestring[start] == ord('l'):
        result = []
        new_start = start + 1
        while bytestring[new_start] != ord('e'):
            (item, new_start) = __bdecode(bytestring, new_start)
            result.append(item)
        new_start += 1
    elif bytestring[start] == ord('d'):
        result = OrderedDict()
        new_start = start + 1
        while bytestring[new_start] != ord('e'):
            (key, new_start) = __decode_string(bytestring, new_start)
            (value, new_start) = __bdecode(bytestring, new_start)
            result[key] = value
        new_start += 1
    else:
        raise ValueError("invalid byte 0x%x at position %d" %
                         (bytestring[start], start))

    return result, new_start

def bdecode(bytestring):
    """ Bdecodes a bytestring """
    try:
        out, end = __bdecode(bytestring, 0)
    except IndexError as e:
        raise ValueError("truncated bencoded bytestring") from e

    if end < len(bytestring):
        raise ValueError("trailing data")

    return out

def bencode(obj):
    """ Bencodes a object """
    if isinstance(obj, int):
        return b'i' + str(obj).encode() + b'e'
    elif isinstance(obj, str):
        return bencode(obj.encode("utf-8"))
    elif isinstance(obj, bytes):
        return str(len(obj)).encode("ascii") + b':' + obj
    elif isinstance(obj, list):
        return b'l' + b''.join([bencode(o) for o in obj]) + b'e'
    elif isinstance(obj, dict):
        return b'd' + b''.join([bencode(key) + bencode(obj[key])
                                for key in obj.keys()]) + b'e'
    else:
        raise TypeError("invalid type for bencoding: %s" % type(obj))
