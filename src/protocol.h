#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

// topic -> partitions -> records (offset = index into inner vector)
extern std::map<std::string, std::vector<std::vector<std::string>>> store;

void sendApiVerResponse(int clientSocket, int32_t correlationId, int16_t apiVersion);
void sendProduceResponse(int clientSocket, int32_t correlationId, const std::string& topicName, int32_t partitionIndex);
void sendMetadataResponse(int clientSocket, int32_t correlationId);

uint64_t readVarint(const std::vector<char>& buf, int& off);
int64_t decodeZigzag(uint64_t original);
