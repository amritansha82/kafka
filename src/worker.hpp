#pragma once
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <string>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <cstdint>
#include <vector>
#include "safe_queue.hpp"
#include <sys/epoll.h>
void worker_run(int epoll_fd);
void io_worker_loop();