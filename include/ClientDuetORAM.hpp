#ifndef CLIENT_DUET_ORAM_HPP_
#define CLIENT_DUET_ORAM_HPP_

#include "config.h"
#include <pthread.h>
#include "zmq.hpp"
#include "struct_socket.h"
#include "SecretSharedShuffle.hpp"

class ClientDuetORAM {
private:
    //client storage for ORAM operations
    TYPE_ID** metaData;
    TYPE_POS_MAP* pos_map;
    block k1;
    block k2;


    //variables for retrieval operation
    TYPE_INDEX numRead;
    uint8_t** sharedVector;             // 客户端给两个服务器生成的查询向量，share，2  *  Z*(H+1)

    TYPE_DATA** retrievedShare;             // 客户端用于接收两个服务器返回的检索结果，2  *  DATA_CHUNKS
    TYPE_DATA* recoveredBlock;              // 客户端恢复查询block，DATA_CHUNKS个

    unsigned char** vector_buffer_out;      //sending retrieval vector buffer, path ID & sharedVector
    unsigned char** block_buffer_out;       // sending writen block (accessed block) buffer to servers
    unsigned char** blocks_buffer_in;       // receiving retrieved blocks buffer from servers



    //variables for eviction
    TYPE_INDEX numEvict;
public:
    ClientDuetORAM();
    ~ClientDuetORAM();


    // main functions
    int init();
    

    //logging
    static unsigned long int exp_logs[9]; 	
    static unsigned long int thread_max;
	static char timestamp[16];
    
};

#endif // CLIENT_DUET_ORAM_HPP_