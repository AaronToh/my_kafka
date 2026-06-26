FROM ubuntu:24.04

RUN apt-get update && apt-get install -y cmake g++ && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

RUN cmake -S . -B build && cmake --build build

EXPOSE 9092

CMD ["./build/my_kafka"]
