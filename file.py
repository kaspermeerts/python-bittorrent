import hashlib
import os
import mmap
from bitarray import bitarray

BLOCKSIZE = 16 * 1024
# TODO Should it though? Maybe just for performance? Will other clients accept
# strange blocksizes?
# assert BLOCKSIZE & (BLOCKSIZE - 1) == 0


class Piece:
    def __init__(self, size, hash, filemap, offset):
        self.size = size
        self.hash = hash
        self.filemap = filemap
        self.offset = offset
        self.verified = False

        self.num_blocks = (size + BLOCKSIZE - 1) // BLOCKSIZE
        self.block_progress = bitarray([False] * self.num_blocks)

    def verify(self):
        piece = self.filemap[self.offset : self.offset+self.size]
        sha = hashlib.sha1(piece).hexdigest()
        self.verified = sha == self.hash
        if self.verified:
            try:
                self.filemap.flush(self.offset, self.size)
            except OSError:
                # Sometimes flushing fails because the address isn't aligned to
                # pagesize. We can safely ignore that
                pass
            self.block_progress = None
        else:
            self.block_progress = bitarray([False] * self.num_blocks)

        print("Verified? %s" % self.verified)

        return self.verified

    def read_block(self, begin, length):
        assert self.verified, "don't serve unverified data"
        assert begin + length <= self.size, "write across piece boundary"

        return self.filemap[self.offset + begin : self.offset + begin + length]

    def get_block_length(self, index):
        if self.size % BLOCKSIZE != 0 and index == self.num_blocks - 1:
            return self.size % BLOCKSIZE
        else:
            return BLOCKSIZE

    def store_block(self, begin, block):
        assert not self.verified, "write to an already verified block"
        assert begin % BLOCKSIZE == 0, "unaligned write"
        block_idx = begin // BLOCKSIZE

        assert len(block) == self.get_block_length(block_idx)

        self.filemap[self.offset + begin:
                     self.offset + begin + len(block)] = block

        self.block_progress[block_idx] = True

        if self.block_progress.all():
            self.verify()

        print("Block written ^_^")


class File:
    def __init__(self, filename, filesize, piece_size, hash_string):
        self.num_pieces = (filesize + piece_size - 1) // piece_size
        assert len(hash_string) // 20 == self.num_pieces
        assert len(hash_string) % 20 == 0

        self.filename = filename
        self.filesize = filesize

        fileno = os.open(self.filename, os.O_CREAT | os.O_RDWR)
        os.ftruncate(fileno, self.filesize)
        self.filemap = mmap.mmap(fileno, self.filesize)
        os.close(fileno)

        piece_hashes = [hash_string[i:i+20].hex()
                        for i in range(0, len(hash_string), 20)]

        self.pieces = []
        for index in range(self.num_pieces):
            if filesize % piece_size != 0 and index == self.num_pieces - 1:
                size = filesize % piece_size  # Trailing piece
            else:
                size = piece_size
            offset = piece_size * index
            piece = Piece(size, piece_hashes[index], self.filemap, offset)
            self.pieces.append(piece)

    def __del__(self):
        self.filemap.flush()
        self.filemap.close()

    def get_bitfield(self):
        bitfield = bitarray([piece.verified for piece in self.pieces])
        bitfield.fill()
        return bitfield

    def verify(self):
        verified = [piece.verify() for piece in self.pieces]
        print("%d verified out of %d" % (sum(verified), len(verified)))

    def store_block(self, index, begin, block):
        assert index < self.num_pieces
        return self.pieces[index].store_block(begin, block)

    def read_block(self, index, begin, length):
        assert index < self.num_pieces
        return self.pieces[index].read_block(begin, length)
