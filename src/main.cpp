#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <map>
#include <netinet/in.h>
#include <unistd.h>
#include <vector>
#include <sys/epoll.h>
#include <sys/socket.h>

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

void sendApiVerResponse(int clientSocket, int32_t correlationId, int16_t apiVersion) {
    if (apiVersion <= 2) {
        char response[26];
        char* p = response;
        int32_t respLen = htonl(22);
        memcpy(p, &respLen, 4); p += 4;
        int32_t corrId = htonl(correlationId);
        memcpy(p, &corrId, 4); p += 4;
        int16_t error = htons(0);
        memcpy(p, &error, 2); p += 2;
        int32_t arrLength = htonl(2);
        memcpy(p, &arrLength, 4); p += 4;
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
        send(clientSocket, response, 26, 0);
    } else {
        // compact arrLength=3 means 2 entries: Metadata(3) and ApiVersions(18)
        char response[30];
        char* p = response;
        int32_t respLen = htonl(26);
        memcpy(p, &respLen, 4); p += 4;
        int32_t corrId = htonl(correlationId);
        memcpy(p, &corrId, 4); p += 4;
        int16_t error = htons(0);
        memcpy(p, &error, 2); p += 2;
        uint8_t arrLength = 3;
        memcpy(p, &arrLength, 1); p += 1;
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
        send(clientSocket, response, 30, 0);
    }
}

void sendMetadataResponse(int clientSocket, int32_t correlationId) {
    // body: corrId(4) + brokers_count(4) + node_id(4) + host_len(2) + "localhost"(9) + port(4) + topics_count(4) = 31
    char response[35];
    char* p = response;
    int32_t respLen = htonl(31);
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
    int32_t topicCount = htonl(0);
    memcpy(p, &topicCount, 4); p += 4;
    send(clientSocket, response, 35, 0);
}

void handleMessage(int clientSocket, int epollfd, std::map<int, ConnectionState>& connections, std::vector<char>& msgBuffer) {
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

    if (apiKey == 18) {
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
        sendMetadataResponse(clientSocket, correlationId);
    } else {
        closeConnection(clientSocket, epollfd, connections);
    }
}

int main() {
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
                        handleMessage(clientSocket, epollfd, connections, state.msgBuffer);
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
