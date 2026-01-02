#ifndef CLIENT_DUET_ORAM_HPP_
#define CLIENT_DUET_ORAM_HPP_

#include "config.h"
#include <pthread.h>
#include "zmq.hpp"
#include "struct_socket.h"
#include "SecretSharedShuffle.hpp"
#include "struct_block.h"

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


    //thread
	pthread_t thread_sockets[NUM_SERVERS];



    //variables for eviction
    TYPE_INDEX numEvict;
    TYPE_INDEX* u1;
    TYPE_INDEX* u2;
    block* permutationAa;       //punctured matrix of A
    block* permutationBb;       //punctured matrix of B
    TYPE_INDEX* sub_pi_1;
    TYPE_INDEX* sub_pi_2;
    TYPE_INDEX* circularShift_1;
    TYPE_INDEX* circularShift_2;
    block** key_permutation_buffer_out;   //buffer to send permutation keys to servers
    TYPE_INDEX* evict_pi;              // permutation indices for eviction
    TYPE_INDEX SIZE_PI;

    unsigned char** evict_buffer_out;



    // preprocessed data for eviction   //NOTE: offline process
    vector<struct_block> blocks;    //store blocks that need to be eviction from the path pathID
    block key3;                     // secret key for permutation
    block key4;                     // secret key for permutation


public:
    ClientDuetORAM();
    ~ClientDuetORAM();


    // main functions
    int init();
    int load();
    int access(TYPE_ID blockID);
    int sendORAMTree();
    int sendInitialPermutation(block** key_permutation_buffer_out);


    // read blocks
    TYPE_INDEX P(TYPE_INDEX pathID, int l);
    TYPE_INDEX getSibling(TYPE_INDEX pIDl);
    int readBucket(TYPE_INDEX bucketID);
    int readSiblingBucket(TYPE_INDEX bucketID);


    //retrieval vector
    int getLogicalVector(uint8_t* logicVector, TYPE_ID blockID);


    //eviction
    int createSubPermutation(TYPE_INDEX* pi_1, TYPE_INDEX* pi_2);
    void generateRandomPermutation(TYPE_INDEX* permutation, TYPE_INDEX size);
    void inversePermutation(TYPE_INDEX* permutation, TYPE_INDEX* inverse, TYPE_INDEX size);


    // socket
    static void* thread_socket_func(void* args);
    static int sendNrecv(std::string ADDR, unsigned char* data_out, size_t data_out_size, unsigned char* data_in, size_t data_in_size, int CMD);
    

    //logging
    static unsigned long int exp_logs[9]; 	
    static unsigned long int thread_max;
	static char timestamp[16];
    
};

#endif // CLIENT_DUET_ORAM_HPP_