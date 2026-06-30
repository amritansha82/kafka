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
#include <algorithm>
#include "codec.hpp"
#include "bodies.hpp"
#include "headers.hpp"
#include "metadata.hpp"

extern std::string server_log_dir;

Message create_response(Message& request);