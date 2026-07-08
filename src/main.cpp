#include <cstring>
#include <endian.h>
#include <fcntl.h>
#include <iostream>
#include <map>
#include <netinet/in.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>
#include <sys/epoll.h>
#include <sys/socket.h>

#include "protocol.h"
#include "ring_buffer.h"

struct ConnectionState {
    size_t li = 0;
    size_t mi = 0;
    std::vector<char> lenBuffer = std::vector<char>(4);
    std::vector<char> msgBuffer;
};

void closeConnection(int clientSocket, int epollfd, std::map<int, ConnectionState>& connections) {
    close(clientSocket);
    epoll_ctl(epollfd, EPOLL_CTL_DEL, clientSocket, nullptr);
    connections.erase(clientSocket);
}

void handleMessage(RingBuffer<QueueItem>& queue, int clientSocket, int epollfd, std::map<int, ConnectionState>& connections, std::vector<char>& msgBuffer) {
    std::cout << "handleMessage called\n";
    std::flush(std::cout);
    int16_t apiKey, apiVersion, clientIdLen;
    int32_t correlationId;

    memcpy(&apiKey, msgBuffer.data(), 2);
    apiKey = ntohs(apiKey);
    memcpy(&apiVersion, msgBuffer.data() + 2, 2);
    apiVersion = ntohs(apiVersion);
    memcpy(&correlationId, msgBuffer.data() + 4, 4);
    correlationId = ntohl(correlationId);
    memcpy(&clientIdLen, msgBuffer.data() + 8, 2);
    clientIdLen = ntohs(clientIdLen);
    char clientId[clientIdLen + 1];
    memset(clientId, 0, clientIdLen + 1);
    if (clientIdLen > 0) memcpy(clientId, msgBuffer.data() + 10, clientIdLen);
    int reqBodyOff = 10 + clientIdLen;
    if (apiVersion > 2) reqBodyOff += 1; // ignore tagged fields

    std::cout << "apiKey: " << apiKey << "\n";
    std::cout << "apiVersion: " << apiVersion << "\n";
    std::cout << "correlationId: " << correlationId << "\n";
    std::cout << "clientId: " << clientId << "\n";

    if (apiKey == 0) {
        std::cout << "parsing produce, msgBuffer size=" << msgBuffer.size() << "\n";
        std::cout << "hex: ";
        for (size_t i = 0; i < msgBuffer.size(); i++)
            printf("%02x ", (uint8_t)msgBuffer[i]);
        printf("\n");
        int16_t transIdLen;
        memcpy(&transIdLen, msgBuffer.data() + reqBodyOff, 2);
        reqBodyOff += 2;
        transIdLen = ntohs(transIdLen);
        char transId[transIdLen + 1]; // currently unused
        memset(transId, 0, transIdLen + 1);
        if (transIdLen > 0) memcpy(transId, msgBuffer.data() + reqBodyOff, transIdLen);
        reqBodyOff += transIdLen;
        int16_t acks;
        memcpy(&acks, msgBuffer.data() + reqBodyOff, 2);
        reqBodyOff += 2;
        int32_t timeout;
        memcpy(&timeout, msgBuffer.data() + reqBodyOff, 4);
        reqBodyOff += 4;
        int32_t topicCount;
        memcpy(&topicCount, msgBuffer.data() + reqBodyOff, 4);
        reqBodyOff += 4;
        topicCount = ntohl(topicCount);
        std::cout << "topicCount: " << topicCount << "\n";
        for (int i = 0; i < topicCount; i++) {
            int16_t topicNameLen;
            memcpy(&topicNameLen, msgBuffer.data() + reqBodyOff, 2);
            reqBodyOff += 2;
            topicNameLen = ntohs(topicNameLen);
            std::string topicName(msgBuffer.data() + reqBodyOff, topicNameLen);
            reqBodyOff += topicNameLen;
            std::cout << "topic: " << topicName << "\n";
            int32_t partitionCount;
            memcpy(&partitionCount, msgBuffer.data() + reqBodyOff, 4);
            reqBodyOff += 4;
            partitionCount = ntohl(partitionCount);
            for (int j = 0; j < partitionCount; j++) {
                int32_t partitionIndex;
                memcpy(&partitionIndex, msgBuffer.data() + reqBodyOff, 4);
                reqBodyOff += 4;
                partitionIndex = ntohl(partitionIndex);
                int32_t recordsLength;
                memcpy(&recordsLength, msgBuffer.data() + reqBodyOff, 4);
                reqBodyOff += 4;
                recordsLength = ntohl(recordsLength);
                std::vector<char> recordsBuffer(msgBuffer.data() + reqBodyOff, msgBuffer.data() + reqBodyOff + recordsLength);
                QueueItem item = QueueItem(clientSocket, correlationId, topicName, partitionIndex, recordsBuffer);
                queue.push(item);
                reqBodyOff += recordsLength;
            }
        }
    } else if (apiKey == 18) {
        sendApiVerResponse(clientSocket, correlationId, apiVersion);
    } else if (apiKey == 3) {
        std::vector<std::string> names;
        if (apiVersion <= 2) {
            int32_t arrLength;
            memcpy(&arrLength, msgBuffer.data() + reqBodyOff, 4);
            arrLength = ntohl(arrLength);
            reqBodyOff += 4;
            names.resize(arrLength); // Todo: handle -1 length
            for (int i = 0; i < arrLength; i++) {
                int16_t nameLen;
                memcpy(&nameLen, msgBuffer.data() + reqBodyOff, 2);
                reqBodyOff += 2;
                nameLen = ntohs(nameLen);
                char name[nameLen + 1];
                memset(name, 0, nameLen + 1);
                if (nameLen > 0) memcpy(name, msgBuffer.data() + reqBodyOff, nameLen);
                names[i] = std::string(name, nameLen);
                reqBodyOff += nameLen;
            }
        } else {
            uint8_t arrLength = msgBuffer[reqBodyOff++];
            arrLength--;
            names.resize(arrLength);
            for (int i = 0; i < arrLength; i++) {
                uint8_t nameLen = msgBuffer[reqBodyOff++];
                nameLen--;
                char name[nameLen + 1];
                memset(name, 0, nameLen + 1);
                if (nameLen > 0) memcpy(name, msgBuffer.data() + reqBodyOff, nameLen);
                names[i] = std::string(name, nameLen);
                reqBodyOff += nameLen;
                reqBodyOff += 1; // tagged field
            }
        }
        for (auto& name : names) {
            if (!name.empty() && store.find(name) == store.end())
                store[name] = std::vector<std::vector<std::string>>(1); // 1 partition
        }
        sendMetadataResponse(clientSocket, correlationId);
    } else {
        std::cout << "unhandled apiKey: " << apiKey << ", closing\n";
        closeConnection(clientSocket, epollfd, connections);
    }
}

int main() {
    RingBuffer<QueueItem> queue(20);
    std::jthread worker([&queue] {
        while (true) {
            QueueItem item = queue.pop();
            handleQueueItem(item);
        }
    });

    setbuf(stdout, NULL);
    int serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSocket == -1) {
        std::cerr << "Socket: " << strerror(errno) << "\n";
        return 1;
    }

    sockaddr_in serverAddress;
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(9092);
    serverAddress.sin_addr.s_addr = INADDR_ANY;

    int opt = 1;
    setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    if (bind(serverSocket, (struct sockaddr*)&serverAddress, sizeof(serverAddress)) == -1) {
        std::cerr << "Bind: " << strerror(errno) << "\n";
        return 1;
    }

    if (listen(serverSocket, 5) == -1) {
        std::cerr << "Listen: " << strerror(errno) << "\n";
        return 1;
    }

    int epollfd = epoll_create1(0);
    if (epollfd == -1) {
        std::cerr << "Create epoll: " << strerror(errno) << "\n";
        return 1;
    }

    epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = serverSocket;
    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, serverSocket, &ev) == -1) {
        std::cerr << "Configure epoll: " << strerror(errno) << "\n";
    }

    const int MAX_EVENTS = 10;
    std::map<int, ConnectionState> connections;

    while (true) {
        epoll_event events[MAX_EVENTS];
        std::cout << "waiting for events\n";
        int n = epoll_wait(epollfd, events, MAX_EVENTS, -1);
        if (n == -1) {
            std::cerr << "Wait: " << strerror(errno) << "\n";
            break;
        }

        for (int i = 0; i < n; i++) {
            if (events[i].data.fd == serverSocket) {
                int clientSocket = accept(serverSocket, nullptr, nullptr);
                if (clientSocket == -1) {
                    std::cerr << "Accept: " << strerror(errno) << "\n";
                    continue;
                }
                int flags = fcntl(clientSocket, F_GETFL, 0);
                fcntl(clientSocket, F_SETFL, flags | O_NONBLOCK);
                epoll_event cev;
                cev.events = EPOLLIN;
                cev.data.fd = clientSocket;
                if (epoll_ctl(epollfd, EPOLL_CTL_ADD, clientSocket, &cev) == -1) {
                    std::cerr << "Interest epoll: " << strerror(errno) << "\n";
                }
                connections[clientSocket] = ConnectionState{};
            } else {
                int clientSocket = events[i].data.fd;
                ConnectionState& state = connections[clientSocket];
                std::cout << "event fd=" << clientSocket << " li=" << state.li << " mi=" << state.mi << "\n";

                if (state.li < state.lenBuffer.size()) {
                    ssize_t bytesRead = recv(clientSocket, state.lenBuffer.data() + state.li, state.lenBuffer.size() - state.li, 0);
                    std::cout << "lenBuf bytesRead=" << bytesRead << " errno=" << errno << "\n";
                    if (bytesRead == 0) { closeConnection(clientSocket, epollfd, connections); continue; }
                    if (bytesRead < 0) { if (errno != EAGAIN) closeConnection(clientSocket, epollfd, connections); continue; }
                    state.li += bytesRead;
                    if (state.li == state.lenBuffer.size()) {
                        int32_t msgLength;
                        memcpy(&msgLength, state.lenBuffer.data(), 4);
                        msgLength = ntohl(msgLength);
                        std::cout << "msgLength=" << msgLength << "\n";
                        state.msgBuffer.resize(msgLength);
                    }
                }

                if (state.li == state.lenBuffer.size() && !state.msgBuffer.empty()) {
                    ssize_t bytesRead = recv(clientSocket, state.msgBuffer.data() + state.mi, state.msgBuffer.size() - state.mi, 0);
                    std::cout << "msgBuf bytesRead=" << bytesRead << " errno=" << errno << "\n";
                    if (bytesRead == 0) { closeConnection(clientSocket, epollfd, connections); continue; }
                    if (bytesRead < 0) { if (errno != EAGAIN) closeConnection(clientSocket, epollfd, connections); continue; }
                    state.mi += bytesRead;
                    if (state.mi == state.msgBuffer.size()) {
                        handleMessage(queue, clientSocket, epollfd, connections, state.msgBuffer);
                        state.li = 0;
                        state.mi = 0;
                        state.msgBuffer.clear();
                    }
                }
            }
        }
    }

    return 0;
}
