import socket
import struct
import sys


def encode_varint(value):
    """Unsigned LEB128 varint, matching readVarint() in protocol.cpp."""
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            break
    return bytes(out)


def encode_zigzag(value):
    """Matches decodeZigzag()'s inverse, for signed varint fields."""
    if value >= 0:
        return (value << 1)
    else:
        return (-value << 1) - 1


def build_record_batch(value_bytes):
    """One RecordBatch (magic=2) containing exactly one record, matching
    the byte layout handleQueueItem's magic==2 branch expects."""
    buf = bytearray()
    buf += struct.pack(">q", 0)       # baseOffset (unused by parser)
    buf += struct.pack(">i", 0)       # batchLength (unused)
    buf += struct.pack(">i", 0)       # partitionLeaderEpoch (unused)
    buf += struct.pack(">b", 2)       # magic = 2 (RecordBatch)
    buf += struct.pack(">I", 0)       # crc (unused)
    buf += struct.pack(">h", 0)       # attributes (unused)
    buf += struct.pack(">i", 0)       # lastOffsetDelta (unused)
    buf += struct.pack(">q", 0)       # baseTimestamp (unused)
    buf += struct.pack(">q", 0)       # maxTimestamp (unused)
    buf += struct.pack(">q", 0)       # producerId (unused)
    buf += struct.pack(">h", 0)       # producerEpoch (unused)
    buf += struct.pack(">i", 0)       # baseSequence (unused)
    buf += struct.pack(">i", 1)       # recordCount = 1

    record = bytearray()
    record += encode_varint(0)                       # recordLength (discarded by parser)
    record += struct.pack(">b", 0)                   # attributes INT8
    record += encode_varint(encode_zigzag(0))         # timestampDelta
    record += encode_varint(0)                        # offsetDelta
    record += encode_varint(encode_zigzag(-1))        # keyLength = -1 (null key)
    record += encode_varint(encode_zigzag(len(value_bytes)))  # valueLength
    record += value_bytes
    record += encode_varint(0)                        # headersCount = 0

    buf += record
    return bytes(buf)


def build_produce_request(correlation_id, client_id, topic_name, partition_values):
    """partition_values: list of (partitionIndex, value_bytes) for ONE topic,
    all bundled into a single Produce request so it's split across multiple
    writeIndex slots in one ProduceRequestState."""
    body = bytearray()
    body += struct.pack(">h", 0)                 # apiKey = 0 (Produce)
    body += struct.pack(">h", 3)                 # apiVersion = 3
    body += struct.pack(">i", correlation_id)
    body += struct.pack(">h", len(client_id))
    body += client_id
    body += struct.pack(">b", 0)                 # tagged fields byte (apiVersion > 2)

    body += struct.pack(">h", 0)                 # transactionalId length = 0 (empty, not null -- avoids the -1 length quirk)
    body += struct.pack(">h", 1)                 # acks = 1
    body += struct.pack(">i", 1000)               # timeout ms
    body += struct.pack(">i", 1)                 # topicCount = 1

    body += struct.pack(">h", len(topic_name))
    body += topic_name
    body += struct.pack(">i", len(partition_values))  # partitionCount

    for partition_index, value_bytes in partition_values:
        records = build_record_batch(value_bytes)
        body += struct.pack(">i", partition_index)
        body += struct.pack(">i", len(records))
        body += records

    framed = struct.pack(">i", len(body)) + bytes(body)
    return framed


def main():
    host = "localhost"
    port = 9092
    topic_name = b"raw-multi-part-topic"
    partition_values = [
        (0, b"hand-crafted value for partition 0"),
        (1, b"hand-crafted value for partition 1"),
    ]

    request = build_produce_request(
        correlation_id=777,
        client_id=b"raw-test-client",
        topic_name=topic_name,
        partition_values=partition_values,
    )

    with socket.create_connection((host, port), timeout=5) as sock:
        sock.sendall(request)
        sock.settimeout(3)
        try:
            response = sock.recv(4096)
            print(f"Received {len(response)} bytes back:")
            print(response.hex(" "))
        except socket.timeout:
            print("No response received within timeout.", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
