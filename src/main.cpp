#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <sys/socket.h>

ssize_t recvAll(int socket, char* buffer, int32_t len) {
    int32_t total = 0;
    while (total < len) {
        ssize_t n = recv(socket, buffer + total, len - total, 0);
        if (n <= 0) return 0;
        total += n;
    }
    return total;
}

int main() {
    int serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSocket == -1) {
        std::cerr << "Socket: " << strerror(errno) << "\n";
        return 1;
    }

    sockaddr_in serverAddress;
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(9092);
    serverAddress.sin_addr.s_addr = INADDR_ANY; // listen on all interfaces - wifi, IPv6
    
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
    while (true) {
        epoll_event events[MAX_EVENTS];
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
                epoll_event ev;
                ev.events = EPOLLIN;
                ev.data.fd = clientSocket;
                if (epoll_ctl(epollfd, EPOLL_CTL_ADD, clientSocket, &ev) == -1) {
                    std::cerr << "Interest epoll: " << strerror(errno) << "\n";
                }
            } else {
                int clientSocket = events[i].data.fd;
                char lenBuf[4];
                if (recvAll(clientSocket, lenBuf, 4) <= 0) {
                    close(clientSocket);
                    epoll_ctl(epollfd, EPOLL_CTL_DEL, clientSocket, nullptr);
                    continue;
                }

                int32_t msg_length;
                memcpy(&msg_length, lenBuf, 4);
                msg_length = ntohl(msg_length);

                char msg[msg_length];
                if (recvAll(clientSocket, msg, msg_length) <= 0) {
                    close(clientSocket);
                    epoll_ctl(epollfd, EPOLL_CTL_DEL, clientSocket, nullptr);
                    continue;
                }

                int16_t api_key, api_version, client_id_len;
                int32_t correlation_id;

                memcpy(&api_key, msg + 0, 2);
                api_key = ntohs(api_key);
                memcpy(&api_version, msg + 2, 2);
                api_version = ntohs(api_version);
                memcpy(&correlation_id, msg + 4, 4);
                correlation_id = ntohl(correlation_id);
                memcpy(&client_id_len, msg + 8, 2);
                client_id_len = ntohs(client_id_len);
                char client_id[client_id_len + 1];
                memset(client_id, 0, client_id_len + 1);
                if (client_id_len > 0) memcpy(client_id, msg + 10, client_id_len);

                std::cout << "api_key: " << api_key << "\n";
                std::cout << "api_version: " << api_version << "\n";
                std::cout << "correlation_id: " << correlation_id << "\n";
                std::cout << "client_id: " << client_id << "\n";

                if (api_key == 18) {
                    if (api_version <= 2) {
                        char response[20];
                        int32_t resp_len = htonl(16);
                        memcpy(response, &resp_len, 4);
                        correlation_id = htonl(correlation_id);
                        memcpy(response + 4, &correlation_id, 4);
                        int16_t error = htons(0);
                        memcpy(response + 8, &error, 2);
                        int32_t arr_length = htonl(1);
                        memcpy(response + 10, &arr_length, 4);
                        int16_t api_key = htons(18);
                        memcpy(response + 14, &api_key, 2);
                        int16_t min_version = htons(0);
                        memcpy(response + 16, &min_version, 2);
                        int16_t max_version = htons(0);
                        memcpy(response + 18, &max_version, 2);
                        send(clientSocket, response, 20, 0);
                    } else {
                        char response[23];
                        int32_t resp_len = htonl(19);
                        memcpy(response, &resp_len, 4);
                        correlation_id = htonl(correlation_id);
                        memcpy(response + 4, &correlation_id, 4);
                        int16_t error = htons(0);
                        memcpy(response + 8, &error, 2);
                        uint8_t arr_length = 2;
                        memcpy(response + 10, &arr_length, 1);
                        int16_t api_key = htons(18);
                        memcpy(response + 11, &api_key, 2);
                        int16_t min_version = htons(0);
                        memcpy(response + 13, &min_version, 2);
                        int16_t max_version = htons(3);
                        memcpy(response + 15, &max_version, 2);
                        uint8_t entry_tags = 0;
                        memcpy(response + 17, &entry_tags, 1);
                        int32_t throttle_time_ms = htonl(0);
                        memcpy(response + 18, &throttle_time_ms, 4);
                        uint8_t resp_tags = 0;
                        memcpy(response + 22, &resp_tags, 1);
                        send(clientSocket, response, 23, 0);
                    }
                } else {
                    close(clientSocket);
                    epoll_ctl(epollfd, EPOLL_CTL_DEL, clientSocket, nullptr);
                }
            }
        }
    }

    return 0;
}
