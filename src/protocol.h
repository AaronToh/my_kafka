#pragma once

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

// topic -> partition count
extern std::map<std::string, size_t> topicDirectory;

void sendApiVerResponse(int clientSocket, int32_t correlationId, int16_t apiVersion);
void sendProduceResponse(int clientSocket, int32_t correlationId, std::vector<std::tuple<std::string, int32_t, size_t>>& results);
void sendMetadataResponse(int clientSocket, int32_t correlationId);

uint64_t readVarint(const std::vector<char>& buf, int& off);
int64_t decodeZigzag(uint64_t original);

struct ProduceRequestState {
    int clientSocket;
    int32_t correlationId;
    std::atomic<int> remaining;
    // topicName, partitionIndex, baseOffset
    std::vector<std::tuple<std::string, int32_t, size_t>> results;

    ProduceRequestState(int clientSocket, int32_t correlationId, int requestsCount)
        : clientSocket(clientSocket),
          correlationId(correlationId),
          remaining(requestsCount),
          results(requestsCount)
    {}
};

struct QueueItem {
    std::shared_ptr<ProduceRequestState> sharedState;
    size_t writeIndex;
    std::string topicName;
    int32_t partitionIndex;
    std::vector<char> recordsBuffer;

    QueueItem(std::shared_ptr<ProduceRequestState> sharedState, size_t writeIndex, std::string topicName, int32_t partitionIndex, std::vector<char> recordsBuffer)
        : sharedState(sharedState),
          writeIndex(writeIndex),
          topicName(std::move(topicName)),
          partitionIndex(partitionIndex),
          recordsBuffer(std::move(recordsBuffer))
    {}
};

void handleQueueItem(QueueItem& item, std::map<std::pair<std::string, int32_t>, std::vector<std::string>>& store);
