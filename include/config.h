#ifndef CONFIG_H_
#define CONFIG_H_

#include "vector"
#include <math.h>
#include "fstream"
#include <iostream>
#include "stdio.h"
#include "stdlib.h"
#include <cstring>
#include <bitset>
#include <algorithm>
#include <assert.h>
#include <boost/progress.hpp>
#include <map>
#include <chrono>
#include <thread>

#include <string>
#include <sstream>
#include <bitset>
#include <emp-tool/emp-tool.h>
#include "Utils.hpp"
using namespace emp;
using namespace std;

#define time_now std::chrono::high_resolution_clock::now()

template <typename T>
static inline std::string to_string(T value)
{
    std::ostringstream os ;
    os << value ;
    return os.str() ;
}

//=== OPERATION TYPE ===============================================
enum Operation {READ, WRITE};
//====================================================================

//=== PARAMETERS ============================================================
#define BLOCK_SIZE 128
#define HEIGHT 4
#define BUCKET_SIZE 333 
#define EVICT_RATE 280
const int H = HEIGHT; 

static const unsigned long long P = 1073742353; //prime field - should have length equal to the defined TYPE_DATA
typedef unsigned long long TYPE_DATA;
//==========================================================================

//=== DATA TYPE ===============================================
typedef unsigned long long TYPE_ID;
typedef long long int TYPE_INDEX;
typedef struct type_pos_map
{
	TYPE_INDEX pathID;
	TYPE_INDEX pathIdx;
    block iv1;
    block iv2;
}TYPE_POS_MAP;


#define DATA_CHUNKS BLOCK_SIZE/sizeof(TYPE_DATA)
const TYPE_INDEX PRECOMP_SIZE = BUCKET_SIZE*(2*HEIGHT+1)*BUCKET_SIZE*(2*HEIGHT+1);
const TYPE_INDEX N_leaf = pow(2,H);
const TYPE_INDEX NUM_BLOCK = ((int) (pow(2,HEIGHT-1))*EVICT_RATE-1);
const TYPE_INDEX NUM_NODES = (int) (pow(2,HEIGHT+1)-1);

//====================================================================

//=== SERVER INFO ============================================================
#define NUM_SERVERS 2

	//SERVER IP ADDRESSES
const std::string SERVER_ADDR[NUM_SERVERS] = {"tcp://localhost", "tcp://localhost"};
#define SERVER_PORT 5555        //define the first port to generate incremental ports for client-server /server-server communications

//============================================================================


//=== PATHS ==================================================================
const std::string rootPath = "../data/";

const std::string clientLocalDir = rootPath + "client_local/";
const std::string clientDataDir = rootPath + "client/";
const std::string serverDataDir0 = rootPath + "server0/";
const std::string serverDataDir1 = rootPath + "server1/";
const std::string clientTempPath = rootPath + "client_local/local_data";
const std::string clientDataDir_iv1 = rootPath + "client_iv1/";
const std::string clientDataDir_iv2 = rootPath + "client_iv2/";


const std::string logDir = rootPath + to_string(H) + "_" + to_string(BLOCK_SIZE) + "/" + "log/";
//=============================================================================

//=== SOCKET COMMAND =========================================
#define CMD_SEND_ORAM_TREE        		0x000010
#define CMD_SEND_BLOCK		          	0x00000E 
#define CMD_SEND_INITIALIZATION_PERMUTATION     0x0000FF
#define CMD_END                         0x0000EE
#define CMD_SHARE                       0x0000AA
#define CMD_SEND_KEYS                   0x000020

#define CMD_SEND_EVICT			        0x000040
#define CMD_REQUEST_BLOCK		        0x000050
#define CMD_START_REDUC			        0x000052
#define CMD_SUCCESS                     "OK"
//============================================================


#endif /* CONFIG_H_ */
