#include <cstdio>
#include <netinet/in.h>
#include <sys/socket.h>

int main() {
    int serverSocket = socket(AF_INET, SOCK_STREAM, 0);

    return 0;
}
