#include <cstdio>
#include <netinet/in.h>
#include <sys/socket.h>

int main() {
    int serverSocket = socket(AF_INET, SOCK_STREAM, 1);

    sockaddr_in serverAddress;
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(8080);
    serverAddress.sin_addr.s_addr = INADDR_ANY;
    
    return 0;
}
