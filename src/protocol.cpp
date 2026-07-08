#include "protocol.h"

#include <cstring>
#include <endian.h>
#include <iostream>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/socket.h>

std::map<std::string, std::vector<std::vector<std::string>>> store;

void sendApiVerResponse(int clientSocket, int32_t correlationId, int16_t apiVersion) {
    if (apiVersion <= 2) {
        char response[32];
        char* p = response;
        int32_t respLen = htonl(28);
        memcpy(p, &respLen, 4); p += 4;
        int32_t corrId = htonl(correlationId);
        memcpy(p, &corrId, 4); p += 4;
        int16_t error = htons(0);
        memcpy(p, &error, 2); p += 2;
        int32_t arrLength = htonl(3);
        memcpy(p, &arrLength, 4); p += 4;
        int16_t produceKey = htons(0);
        memcpy(p, &produceKey, 2); p += 2;
        int16_t produceMin = htons(3);
        memcpy(p, &produceMin, 2); p += 2;
        int16_t produceMax = htons(3);
        memcpy(p, &produceMax, 2); p += 2;
        int16_t metaApiKey = htons(3);
        memcpy(p, &metaApiKey, 2); p += 2;
        int16_t metaMin = htons(0);
        memcpy(p, &metaMin, 2); p += 2;
        int16_t metaMax = htons(0);
        memcpy(p, &metaMax, 2); p += 2;
        int16_t apiVerKey = htons(18);
        memcpy(p, &apiVerKey, 2); p += 2;
        int16_t apiVerMin = htons(0);
        memcpy(p, &apiVerMin, 2); p += 2;
        int16_t apiVerMax = htons(0);
        memcpy(p, &apiVerMax, 2); p += 2;
        send(clientSocket, response, 32, 0);
    } else {
        // compact arrLength=4 means 3 entries: Produce(0), Metadata(3), ApiVersions(18)
        char response[37];
        char* p = response;
        int32_t respLen = htonl(33);
        memcpy(p, &respLen, 4); p += 4;
        int32_t corrId = htonl(correlationId);
        memcpy(p, &corrId, 4); p += 4;
        int16_t error = htons(0);
        memcpy(p, &error, 2); p += 2;
        uint8_t arrLength = 4;
        memcpy(p, &arrLength, 1); p += 1;
        int16_t produceKey = htons(0);
        memcpy(p, &produceKey, 2); p += 2;
        int16_t produceMin = htons(3);
        memcpy(p, &produceMin, 2); p += 2;
        int16_t produceMax = htons(3);
        memcpy(p, &produceMax, 2); p += 2;
        uint8_t produceTags = 0;
        memcpy(p, &produceTags, 1); p += 1;
        int16_t metaApiKey = htons(3);
        memcpy(p, &metaApiKey, 2); p += 2;
        int16_t metaMin = htons(0);
        memcpy(p, &metaMin, 2); p += 2;
        int16_t metaMax = htons(0);
        memcpy(p, &metaMax, 2); p += 2;
        uint8_t metaTags = 0;
        memcpy(p, &metaTags, 1); p += 1;
        int16_t apiVerKey = htons(18);
        memcpy(p, &apiVerKey, 2); p += 2;
        int16_t apiVerMin = htons(0);
        memcpy(p, &apiVerMin, 2); p += 2;
        int16_t apiVerMax = htons(3);
        memcpy(p, &apiVerMax, 2); p += 2;
        uint8_t apiVerTags = 0;
        memcpy(p, &apiVerTags, 1); p += 1;
        int32_t throttleTimeMs = htonl(0);
        memcpy(p, &throttleTimeMs, 4); p += 4;
        uint8_t respTags = 0;
        memcpy(p, &respTags, 1); p += 1;
        send(clientSocket, response, 37, 0);
    }
}

void sendProduceResponse(int clientSocket, int32_t correlationId, const std::string& topicName, int32_t partitionIndex) {
    // body: corrId(4) + responses_count(4) + name_len(2) + name + partition_responses_count(4)
    //       + index(4) + error_code(2) + base_offset(8) + log_append_time_ms(8) + throttle_time_ms(4)
    int16_t nameLen = (int16_t)topicName.size();
    int totalSize = 4 + 4 + 2 + nameLen + 4 + 4 + 2 + 8 + 8 + 4;
    std::vector<char> response(4 + totalSize);
    char* p = response.data();
    int32_t respLen = htonl(totalSize);
    memcpy(p, &respLen, 4); p += 4;
    int32_t corrId = htonl(correlationId);
    memcpy(p, &corrId, 4); p += 4;
    int32_t topicCount = htonl(1);
    memcpy(p, &topicCount, 4); p += 4;
    int16_t nameLenN = htons(nameLen);
    memcpy(p, &nameLenN, 2); p += 2;
    memcpy(p, topicName.data(), nameLen); p += nameLen;
    int32_t partCount = htonl(1);
    memcpy(p, &partCount, 4); p += 4;
    int32_t idx = htonl(partitionIndex);
    memcpy(p, &idx, 4); p += 4;
    int16_t errorCode = htons(0);
    memcpy(p, &errorCode, 2); p += 2;
    int64_t baseOffset = 0;
    memcpy(p, &baseOffset, 8); p += 8;
    int64_t logAppendTime = htobe64(-1);
    memcpy(p, &logAppendTime, 8); p += 8;
    int32_t throttle = htonl(0);
    memcpy(p, &throttle, 4); p += 4;
    send(clientSocket, response.data(), response.size(), 0);
}

void sendMetadataResponse(int clientSocket, int32_t correlationId) {
    // size varies by number of topics/partitions so use vector
    // per topic: error(2) + name_len(2) + name + partitions_count(4)
    // per partition: error(2) + index(4) + leader(4) + replicas_count(4) + replica(4) + isr_count(4) + isr(4) = 26
    int bodySize = 4 + 19; // broker_count + broker(node_id+host+port)
    for (auto& [name, partitions] : store)
        bodySize += 2 + 2 + name.size() + 4 + (int)partitions.size() * 26;
    bodySize += 4; // topic_count

    std::vector<char> response(4 + 4 + bodySize);
    char* p = response.data();
    int32_t respLen = htonl(4 + bodySize);
    memcpy(p, &respLen, 4); p += 4;
    int32_t corrId = htonl(correlationId);
    memcpy(p, &corrId, 4); p += 4;
    int32_t brokerCount = htonl(1);
    memcpy(p, &brokerCount, 4); p += 4;
    int32_t nodeId = htonl(0);
    memcpy(p, &nodeId, 4); p += 4;
    int16_t hostLen = htons(9);
    memcpy(p, &hostLen, 2); p += 2;
    memcpy(p, "localhost", 9); p += 9;
    int32_t port = htonl(9092);
    memcpy(p, &port, 4); p += 4;
    int32_t topicCount = htonl((int32_t)store.size());
    memcpy(p, &topicCount, 4); p += 4;
    for (auto& [name, partitions] : store) {
        int16_t topicError = htons(0);
        memcpy(p, &topicError, 2); p += 2;
        int16_t nameLen = htons((int16_t)name.size());
        memcpy(p, &nameLen, 2); p += 2;
        memcpy(p, name.data(), name.size()); p += name.size();
        int32_t partCount = htonl((int32_t)partitions.size());
        memcpy(p, &partCount, 4); p += 4;
        for (int i = 0; i < (int)partitions.size(); i++) {
            int16_t partError = htons(0);
            memcpy(p, &partError, 2); p += 2;
            int32_t partIdx = htonl(i);
            memcpy(p, &partIdx, 4); p += 4;
            int32_t leaderId = htonl(0);
            memcpy(p, &leaderId, 4); p += 4;
            int32_t replicaCount = htonl(1);
            memcpy(p, &replicaCount, 4); p += 4;
            int32_t replicaNode = htonl(0);
            memcpy(p, &replicaNode, 4); p += 4;
            int32_t isrCount = htonl(1);
            memcpy(p, &isrCount, 4); p += 4;
            int32_t isrNode = htonl(0);
            memcpy(p, &isrNode, 4); p += 4;
        }
    }
    send(clientSocket, response.data(), response.size(), 0);
}

uint64_t readVarint(const std::vector<char>& buf, int& off) {
    uint64_t res = 0;
    int shift = 0;
    while (true) {
        uint8_t byte;
        memcpy(&byte, buf.data() + off, 1);
        off++;
        res |= (uint64_t)(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) break;
        shift += 7;
    }
    return res;
}

int64_t decodeZigzag(uint64_t original) {
    if (original % 2 == 0) {
        return original >> 1;
    } else {
        return -(int64_t)(original >> 1) - 1;
    }
}

void handleQueueItem(QueueItem& item) {
    int clientSocket = item.clientSocket;
    int32_t correlationId = item.correlationId;
    std::string topicName = std::move(item.topicName);
    int32_t partitionIndex = item.partitionIndex;
    std::vector<char> recordsBuffer = std::move(item.recordsBuffer);
    int recBodyOff = 0;
    int64_t baseOffset;
    memcpy(&baseOffset, recordsBuffer.data(), 8);
    recBodyOff += 8;
    baseOffset = be64toh(baseOffset);
    int32_t batchLength;
    memcpy(&batchLength, recordsBuffer.data() + recBodyOff, 4);
    recBodyOff += 4;
    batchLength = ntohl(batchLength);
    int32_t partitionLeaderEpoch;
    memcpy(&partitionLeaderEpoch, recordsBuffer.data() + recBodyOff, 4);
    recBodyOff += 4;
    partitionLeaderEpoch = ntohl(partitionLeaderEpoch);
    int8_t magic;
    memcpy(&magic, recordsBuffer.data() + recBodyOff, 1);
    recBodyOff += 1;
    if (magic < 2) {
        while (true) {
            // Old MessageSet format: offset(8)+messageSize(4)+crc(4)+magic(1) already consumed for this message
            recBodyOff += 1; // attributes INT8
            if (magic == 1) recBodyOff += 8; // timestamp INT64
            int32_t keyLen;
            memcpy(&keyLen, recordsBuffer.data() + recBodyOff, 4);
            recBodyOff += 4;
            keyLen = ntohl(keyLen);
            if (keyLen > 0) recBodyOff += keyLen;
            int32_t valueLen;
            memcpy(&valueLen, recordsBuffer.data() + recBodyOff, 4);
            recBodyOff += 4;
            valueLen = ntohl(valueLen);
            if (valueLen > 0) {
                std::string val(recordsBuffer.data() + recBodyOff, valueLen);
                recBodyOff += valueLen;
                std::cout << "value: " << val << "\n";
                if (store.find(topicName) == store.end())
                    store[topicName] = std::vector<std::vector<std::string>>(partitionIndex + 1);
                store[topicName][partitionIndex].push_back(val);
            }

            if (recBodyOff >= (int)recordsBuffer.size()) break;

            // Next message: re-read its own offset(8)+messageSize(4)+crc(4)+magic(1) header
            recBodyOff += 8 + 4 + 4;
            memcpy(&magic, recordsBuffer.data() + recBodyOff, 1);
            recBodyOff += 1;
        }
    } else {
        // RecordBatch format (magic=2)
        uint32_t crc;
        memcpy(&crc, recordsBuffer.data() + recBodyOff, 4);
        recBodyOff += 4;
        crc = ntohl(crc);
        int16_t attributes;
        memcpy(&attributes, recordsBuffer.data() + recBodyOff, 2);
        recBodyOff += 2;
        attributes = ntohs(attributes);
        recBodyOff += 4; // lastOffsetDelta
        recBodyOff += 8; // baseTimestamp
        recBodyOff += 8; // maxTimestamp
        recBodyOff += 8; // producerId
        recBodyOff += 2; // producerEpoch
        recBodyOff += 4; // baseSequence
        int32_t recordCount;
        memcpy(&recordCount, recordsBuffer.data() + recBodyOff, 4);
        recBodyOff += 4;
        recordCount = ntohl(recordCount);
        for (int k = 0; k < recordCount; k++) {
            readVarint(recordsBuffer, recBodyOff); // recordLength
            recBodyOff += 1; // attributes INT8
            decodeZigzag(readVarint(recordsBuffer, recBodyOff)); // timestampDelta
            readVarint(recordsBuffer, recBodyOff); // offsetDelta
            int64_t keyLength = decodeZigzag(readVarint(recordsBuffer, recBodyOff));
            if (keyLength > 0) recBodyOff += keyLength;
            int64_t valueLength = decodeZigzag(readVarint(recordsBuffer, recBodyOff));
            if (valueLength > 0) {
                std::string val(recordsBuffer.data() + recBodyOff, valueLength);
                recBodyOff += valueLength;
                std::cout << "value: " << val << "\n";
                if (store.find(topicName) == store.end())
                    store[topicName] = std::vector<std::vector<std::string>>(partitionIndex + 1);
                store[topicName][partitionIndex].push_back(val);
            }
            uint64_t headersCount = readVarint(recordsBuffer, recBodyOff);
            for (uint64_t l = 0; l < headersCount; l++) {
                int64_t headerKeyLen = decodeZigzag(readVarint(recordsBuffer, recBodyOff));
                if (headerKeyLen > 0) recBodyOff += headerKeyLen;
                int64_t headerValLen = decodeZigzag(readVarint(recordsBuffer, recBodyOff));
                if (headerValLen > 0) recBodyOff += headerValLen;
            }
        }
    }
    // Todo: should be one response for a single request
    sendProduceResponse(clientSocket, correlationId, topicName, partitionIndex);
}
