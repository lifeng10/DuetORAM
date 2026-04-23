#include "ServerDuetORAM.hpp"
#include "config.h"
#include "Utils.hpp"
#include "struct_socket.h"
#include "CPUFeatures.hpp"
#include "AESNIWrapper.hpp"

#include "DuetORAM.hpp"
#include <iostream>
#include <sys/socket.h>
#include <sys/types.h>
#include <immintrin.h>

#include "struct_thread_computation.h"
#include "struct_thread_loadData.h"

unsigned long int ServerDuetORAM::server_logs[27];
unsigned long int ServerDuetORAM::thread_max = 0;
char ServerDuetORAM::timestamp[16];

struct ThreadSocketArgs {
    zmq::socket_t* socket; 
    struct_socket* sockData;
};

ServerDuetORAM::ServerDuetORAM(TYPE_INDEX serverNo, int selectedThreads) {
    this->CLIENT_ADDR = "tcp://*:" + std::to_string(SERVER_PORT+(serverNo)*NUM_SERVERS+serverNo);

    this->numThreads = selectedThreads;
    this->thread_compute = new pthread_t[this->numThreads];

    cout<<endl;
	cout << "=================================================================" << endl;
	cout<< "Starting Server-" << serverNo+1 <<endl;
	cout << "=================================================================" << endl;
	this->serverNo = serverNo;

    this->others = 1 - serverNo;

    // variables for retrieval


    bucket_buffer = new unsigned char[BUCKET_SIZE*sizeof(TYPE_DATA)*DATA_CHUNKS];

    iv_buffer = new unsigned char[BUCKET_SIZE*sizeof(block)];

    key_buffer = new unsigned char[sizeof(block)];


    this->select_buffer_in = new unsigned char[sizeof(TYPE_INDEX)+(H+1)*BUCKET_SIZE*sizeof(uint8_t)];

    this->dot_product_vector = new TYPE_DATA*[DATA_CHUNKS];
	for (TYPE_INDEX k = 0 ; k < DATA_CHUNKS; k++)
	{
		this->dot_product_vector[k] = new TYPE_DATA[BUCKET_SIZE*(H+1)];
	}
    

    sumBlock = new TYPE_DATA[DATA_CHUNKS];



    // socket
    this->SIZE_PI = (H+2)*BUCKET_SIZE;
    this->block_buffer_out = new unsigned char[sizeof(TYPE_DATA)*DATA_CHUNKS];
    this->block_buffer_in = new unsigned char[sizeof(TYPE_DATA)*DATA_CHUNKS+sizeof(TYPE_INDEX)+sizeof(block)];
    this->evict_buffer_in = new unsigned char[sizeof(TYPE_INDEX) + sizeof(block) + 3 * sizeof(TYPE_INDEX) * SIZE_PI];
    this->path_vector_in_for_identical_copy = new unsigned char[DATA_CHUNKS * SIZE_PI * sizeof(TYPE_DATA)];
    this->path_vector_out_for_identical_copy = new unsigned char[DATA_CHUNKS * SIZE_PI * sizeof(TYPE_DATA)];

    // variables for eviction
    this->key_permutation_buffer_in = new block[1 + (H+2)*BUCKET_SIZE * (H+2)*BUCKET_SIZE];


    this-> receivedPuncturedPermutation = new block[(H+2)*BUCKET_SIZE * (H+2)*BUCKET_SIZE];


    this->fullPermutation = new block[(H+2)*BUCKET_SIZE * (H+2)*BUCKET_SIZE];


    this->fullPermutationExpansion = new TYPE_DATA*[DATA_CHUNKS];
    for (TYPE_INDEX i = 0; i < DATA_CHUNKS; i++)
    {
        this->fullPermutationExpansion[i] = new TYPE_DATA[(H+2)*BUCKET_SIZE * (H+2)*BUCKET_SIZE];
    }


    this->puncturedPermutationExpansion = new TYPE_DATA*[DATA_CHUNKS];
    for (TYPE_INDEX i = 0; i < DATA_CHUNKS; i++)
    {
        this->puncturedPermutationExpansion[i] = new TYPE_DATA[(H+2)*BUCKET_SIZE * (H+2)*BUCKET_SIZE];
    }
    
    // eviction, Share Translation
    this->sub_pi = new TYPE_INDEX[SIZE_PI];
    this->circularShift_1 = new TYPE_INDEX[SIZE_PI];
    this->circularShift_2 = new TYPE_INDEX[SIZE_PI];

    this->a = new TYPE_DATA*[DATA_CHUNKS];
    this->b = new TYPE_DATA*[DATA_CHUNKS];
    this->delta = new TYPE_DATA*[DATA_CHUNKS];
    for (TYPE_INDEX i = 0; i < DATA_CHUNKS; i++)
    {
        this->a[i] = new TYPE_DATA[SIZE_PI];
        this->b[i] = new TYPE_DATA[SIZE_PI];
        this->delta[i] = new TYPE_DATA[SIZE_PI];
        memset(this->a[i],0,sizeof(TYPE_DATA)*SIZE_PI);
        memset(this->b[i],0,sizeof(TYPE_DATA)*SIZE_PI);
        memset(this->delta[i],0,sizeof(TYPE_DATA)*SIZE_PI);
    }
    
    time_t rawtime = time(0);
	tm *now = localtime(&rawtime);

	if(rawtime != -1)
		strftime(timestamp,16,"%d%m_%H%M",now);
    
}

ServerDuetORAM::~ServerDuetORAM() {
    
}

int ServerDuetORAM::start() {
    int CMD;
    unsigned char buffer[sizeof(CMD)];
    zmq::context_t context(1);
    zmq::socket_t socket(context,ZMQ_REP);

    cout<< "[Server] Socket is OPEN on " << this->CLIENT_ADDR << endl;
    socket.bind(this->CLIENT_ADDR.c_str());

    auto start_evict = time_now;
    auto end_evict = time_now;
    auto start_recvORAMTree = time_now;
    auto end_recvORAMTree = time_now;
    auto start_retrieve = time_now;
    auto end_retrieve = time_now;
    auto start_recvBlock = time_now;
    auto end_recvBlock = time_now;
    auto start_recvInitializationPermutation = time_now;
    auto end_recvInitializationPermutation = time_now;
    auto start_generatePermutationOffline = time_now;
    auto end_generatePermutationOffline = time_now;

    while(true)
    {
        cout<< "[Server] Waiting for a Command..." <<endl;
        socket.recv(buffer,sizeof(CMD));

        memcpy(&CMD, buffer, sizeof(CMD));
		cout<< "[Server] Command RECEIVED!" <<endl;

        socket.send((unsigned char*)CMD_SUCCESS,sizeof(CMD_SUCCESS));

        switch (CMD)
        {
            case CMD_SEND_ORAM_TREE:
                cout<<endl;
				cout << "=================================================================" << endl;
				cout<< "[Server] Receiving ORAM Data..." <<endl;
				cout << "=================================================================" << endl;
                start_recvORAMTree = time_now;
				this->recvORAMTree(socket);
                end_recvORAMTree = time_now;
                server_logs[21] = std::chrono::duration_cast<std::chrono::nanoseconds>(end_recvORAMTree-start_recvORAMTree).count();
				cout << "=================================================================" << endl;
				cout<< "[Server] ORAM Data RECEIVED!" <<endl;
				cout << "=================================================================" << endl;
				cout<<endl;
				break;
            case CMD_SEND_KEYS:
                cout<<endl;
				cout << "=================================================================" << endl;
				cout<< "[Server] Receiving ORAM Key..." <<endl;
				cout << "=================================================================" << endl;
				this->recvORAMKey(socket);
				cout << "=================================================================" << endl;
				cout<< "[Server] ORAM Key RECEIVED!" <<endl;
				cout << "=================================================================" << endl;
				cout<<endl;
				break;
            case CMD_REQUEST_BLOCK:
				cout<<endl;
				cout << "=================================================================" << endl;
				cout<< "[Server] Receiving Logical Vector..." <<endl;
				cout << "=================================================================" << endl;
                start_retrieve = time_now;
				this->retrieve(socket);
                end_retrieve = time_now;
                server_logs[22] = std::chrono::duration_cast<std::chrono::nanoseconds>(end_retrieve-start_retrieve).count();
				cout << "=================================================================" << endl;
				cout<< "[Server] Block Share SENT" <<endl;
				cout << "=================================================================" << endl;
				cout<<endl;
				break;
            case CMD_SEND_BLOCK:
				cout<<endl;
            	cout << "=================================================================" << endl;
				cout<< "[Server] Receiving Block Data..." <<endl;
				cout << "=================================================================" << endl;
                start_recvBlock = time_now;
				this->recvBlock(socket);
                end_recvBlock = time_now;
                server_logs[23] = std::chrono::duration_cast<std::chrono::nanoseconds>(end_recvBlock-start_recvBlock).count();
				cout << "=================================================================" << endl;
				cout<< "[Server] Block Data RECEIVED!" <<endl;
				cout << "=================================================================" << endl;
				cout<<endl;
				break;
            case CMD_SEND_INITIALIZATION_PERMUTATION:
                cout << endl;
                cout << "=================================================================" << endl;
                cout<< "[Server] Receiving Initial Random Secret Key and Punctured Permutation..." <<endl;
                cout << "=================================================================" << endl;
                start_recvInitializationPermutation = time_now;
                this->recvInitialPermutation(socket);
                end_recvInitializationPermutation = time_now;
                server_logs[24] = std::chrono::duration_cast<std::chrono::nanoseconds>(end_recvInitializationPermutation-start_recvInitializationPermutation).count();
                cout << "=================================================================" << endl;
                cout<< "[Server] Initial Random Secret Key and Punctured Permutation RECEIVED!" <<endl;
                cout << "=================================================================" << endl;
                start_generatePermutationOffline = time_now;
                generatePermutationOffline(keytoPermutation, fullPermutation);
                end_generatePermutationOffline = time_now;
                server_logs[25] = std::chrono::duration_cast<std::chrono::nanoseconds>(end_generatePermutationOffline-start_generatePermutationOffline).count();
                // cout << "[Server] Generated Full Permutation Offline in " << server_logs[25] << " ns" << endl;
                cout << "=================================================================" << endl;
                cout<< "[Server] Full Permutation is Recovered!" <<endl;
                cout << "=================================================================" << endl;
                cout<<endl;
                break;
            case CMD_SEND_EVICT:
				cout<<endl;
				cout << "=================================================================" << endl;
				cout<< "Receiving Eviction Matrix..." <<endl;
				cout << "=================================================================" << endl;
                start_evict = time_now;
				this->evict(socket);
                end_evict = time_now;
                server_logs[26] = std::chrono::duration_cast<std::chrono::nanoseconds>(end_evict-start_evict).count();
                cout<< "	[Server] Eviction Matrix Processed in " << std::chrono::duration_cast<std::chrono::nanoseconds>(end_evict-start_evict).count()<< " ns"<<endl;
				cout << "=================================================================" << endl;
				cout<< "[Server] EVICTION and DEGREE REDUCTION DONE!" <<endl;
				cout << "=================================================================" << endl;
				cout<<endl;
				break;
            
            default:
                break;
        }

        Utils::write_list_to_file(to_string(HEIGHT) + "_" + to_string(BLOCK_SIZE) + "_server" + to_string(serverNo)+ "_" + timestamp + ".txt",logDir, server_logs, 27);
	    memset(server_logs, 0, sizeof(unsigned long int)*27);

    }

    return 0;
}

int ServerDuetORAM::recvORAMTree(zmq::socket_t& socket) {
    for (int i = 0; i < NUM_NODES; i++)
    {
        socket.recv(this->bucket_buffer, BUCKET_SIZE*sizeof(TYPE_DATA)*DATA_CHUNKS, 0);
        string path = rootPath + to_string(this->serverNo) + "/" + to_string(i);

        FILE* file_out = NULL;
        if ((file_out = fopen(path.c_str(), "wb+")) == NULL)
        {
            cout<< "	[recvORAMTree] File Cannot be Opened!!" <<endl;
            exit(0);
        }
        fwrite(this->bucket_buffer, 1, BUCKET_SIZE*sizeof(TYPE_DATA)*DATA_CHUNKS, file_out);
        fclose(file_out);
        socket.send((unsigned char*)CMD_SUCCESS,sizeof(CMD_SUCCESS), 0);
        
        socket.recv(this->iv_buffer, BUCKET_SIZE*sizeof(block), 0);
        string path_iv = rootPath + "client_iv" + to_string(this->serverNo+1) + "/" + to_string(i);
        FILE* file_iv_out = NULL;
        if ((file_iv_out = fopen(path_iv.c_str(), "wb+")) == NULL)
        {
            cout<< "	[recvORAMTree] File Cannot be Opened!!" <<endl;
            exit(0);
        }
        fwrite(this->iv_buffer, 1, BUCKET_SIZE*sizeof(block), file_iv_out);
        fclose(file_iv_out);
        socket.send((unsigned char*)CMD_SUCCESS,sizeof(CMD_SUCCESS), 0);
    }

    socket.recv(this->key_buffer, sizeof(block), 0);
    memcpy(&this->mask_key, key_buffer, sizeof(block));
    string path_key = rootPath + "keys/" + to_string(this->serverNo+1);
    FILE* file_key_out = NULL;
    if ((file_key_out = fopen(path_key.c_str(), "wb+")) == NULL)
    {
        cout<< "	[recvORAMTree] File Cannot be Opened!!" <<endl;
        exit(0);
    }
    fwrite(this->key_buffer, 1, sizeof(block), file_key_out);
    fclose(file_key_out);
    socket.send((unsigned char*)CMD_SUCCESS,sizeof(CMD_SUCCESS),0);
    cout<< "	[recvORAMTree] ACK is SENT!" <<endl;

    return 0;
}

int ServerDuetORAM::recvORAMKey(zmq::socket_t& socket){
    socket.recv((unsigned char*)&mask_key,sizeof(block),0);
    cout<< "	[recvORAMKey] ORAM Key RECEIVED!" <<endl;
    cout << "	[recvORAMKey] ORAM Key is: " << mask_key << endl;
    socket.send((unsigned char*)CMD_SUCCESS,sizeof(CMD_SUCCESS),0);

    cout<< "	[recvORAMTree] ACK is SENT!" <<endl;

    return 0;
}

int ServerDuetORAM::retrieve(zmq::socket_t& socket)
{

    int ret = 1;
	
	auto start = time_now;
    // COMMUNICATION:
	socket.recv(select_buffer_in,sizeof(TYPE_INDEX)+(H+1)*BUCKET_SIZE*sizeof(TYPE_DATA),0);
	auto end = time_now;
	cout<< "	[SendBlock] PathID and Logical Vector RECEIVED in " << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count() << " ns" <<endl;
    server_logs[0] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();

    start = time_now;
    TYPE_INDEX pathID;
	memcpy(&pathID, select_buffer_in, sizeof(pathID));

    uint8_t sharedVector[(H+1)*BUCKET_SIZE];
    memcpy(sharedVector, &select_buffer_in[sizeof(pathID)], (H+1)*BUCKET_SIZE*sizeof(uint8_t));

    cout << "    [SendBlock] Retrieval request for pathID " << pathID << endl;

    DuetORAM ORAM;
	TYPE_INDEX fullPathIdx[H+1];
    ORAM.getFullPathIdx(fullPathIdx, pathID);
    
    cout << "    [SendBlock] Full path: ";
    for (int i = 0; i <= H; i++) {
        cout << fullPathIdx[i] << " ";
    }
    cout << endl;

    // 1. use thread to load data from files
    
    int step = ceil((double)DATA_CHUNKS/(double)numThreads);
    int endIdx;

    THREAD_LOADDATA loadData_args[numThreads];
    for(int i = 0, startIdx = 0; i < numThreads , startIdx < DATA_CHUNKS; i ++, startIdx+=step)
    {
        if(startIdx+step > DATA_CHUNKS)
            endIdx = DATA_CHUNKS;
        else
            endIdx = startIdx+step;
            
        loadData_args[i] = THREAD_LOADDATA(this->serverNo, startIdx, endIdx, this->dot_product_vector, fullPathIdx,H+1);
        pthread_create(&thread_compute[i], NULL, &ServerDuetORAM::thread_loadRetrievalData_func, (void*)&loadData_args[i]);
        
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i, &cpuset);
        pthread_setaffinity_np(thread_compute[i], sizeof(cpu_set_t), &cpuset);
    }
    for(int i = 0; i < numThreads; i++)
    {
        pthread_join(thread_compute[i],NULL);
    }
    end = time_now;
	long load_time = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();
    cout<< "	[SendBlock] Path Nodes READ from Disk in " << load_time << " ns"<<endl;
    server_logs[1] = load_time;

    // 2. compute dot product, Multithread for dot product computation
    start = time_now;
    THREAD_COMPUTATION dotProduct_args[numThreads];
    endIdx = 0;
    step = ceil((double)DATA_CHUNKS/(double)numThreads);

    for(int i = 0, startIdx = 0 ; i < numThreads , startIdx < DATA_CHUNKS; i ++, startIdx+=step)
    {
        if(startIdx+step > DATA_CHUNKS)
            endIdx = DATA_CHUNKS;
        else
            endIdx = startIdx+step;
        
        dotProduct_args[i] = THREAD_COMPUTATION( startIdx, endIdx, this->dot_product_vector, sharedVector, sumBlock);
        pthread_create(&thread_compute[i], NULL, &ServerDuetORAM::thread_dotProduct_func, (void*)&dotProduct_args[i]);

        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i, &cpuset);
        pthread_setaffinity_np(thread_compute[i], sizeof(cpu_set_t), &cpuset);
    }
    
    for(int i = 0; i < numThreads; i++)
    {
        pthread_join(thread_compute[i],NULL);
    }

    end = time_now;
    cout<< "	[SendBlock] Block Share CALCULATED in " << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count() <<endl;
    server_logs[2] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();

    start = time_now;
    memcpy(block_buffer_out,sumBlock,sizeof(TYPE_DATA)*DATA_CHUNKS);

    cout<< "	[SendBlock] Sending Block Share with ID-" << sumBlock[0] <<endl;
    // COMMUNICATION:
    socket.send(block_buffer_out,sizeof(TYPE_DATA)*DATA_CHUNKS);
    end = time_now;
    cout<< "	[SendBlock] Block Share SENT in " << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count() <<endl;
    server_logs[3] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();

    

    return 0;
}

void* ServerDuetORAM::thread_loadRetrievalData_func(void* args)
{
    THREAD_LOADDATA* opt = (THREAD_LOADDATA*) args;

    unsigned long int load_time = 0;
    FILE* file_in = NULL;
    string path;

    for (int i = 0; i < opt->fullPathIdx_length; i++)
    {
        file_in = NULL;
        path = rootPath + to_string(opt->serverNo) + "/" + to_string(opt->fullPathIdx[i]);
        if((file_in = fopen(path.c_str(),"rb")) == NULL){
            cout<< "	[SendBlock] File cannot be opened!!" <<endl;
            exit(0);
        }

        fseek(file_in,BUCKET_SIZE*(opt->startIdx)*sizeof(TYPE_DATA),SEEK_SET);

        for (int k = opt->startIdx ; k < opt->endIdx; k++)
        {
            for(int j = 0 ; j < BUCKET_SIZE; j ++)
            {
                fread(&opt->data_vector[k][i*BUCKET_SIZE+j],1,sizeof(TYPE_DATA),file_in);
            }
        }
        fclose(file_in);
    }
    
}

void* ServerDuetORAM::thread_dotProduct_func(void* args)
{
    THREAD_COMPUTATION* opt = (THREAD_COMPUTATION*) args;
    const int SIZE = BUCKET_SIZE * (H + 1);
    
    for (int i = opt->start; i < opt->end; i++)
    {
        // Strict aliasing: Tell compiler these don't overlap (critical for -O0)
        TYPE_DATA* __restrict__ data_row = opt->data_vector[i];
        uint8_t* __restrict__ mask = opt->sharedVector;
        TYPE_DATA result = 0;
        
        if (CPUFeatures::has_avx2()) {
            __m256i acc = _mm256_setzero_si256();
            
            int j = 0;
            // Process 8×64-bit per iteration (2x unroll for better pipelining)
            for (; j + 7 < SIZE; j += 8) {
                // First 4 elements
                __m128i mask_bytes1 = _mm_set_epi32(
                    -mask[j+3], -mask[j+2], -mask[j+1], -mask[j+0]
                );
                __m256i mask_64_1 = _mm256_cvtepi32_epi64(mask_bytes1);
                __m256i data1 = _mm256_loadu_si256((__m256i*)&data_row[j]);
                __m256i masked1 = _mm256_and_si256(data1, mask_64_1);
                
                // Second 4 elements (pipeline interleave)
                __m128i mask_bytes2 = _mm_set_epi32(
                    -mask[j+7], -mask[j+6], -mask[j+5], -mask[j+4]
                );
                __m256i mask_64_2 = _mm256_cvtepi32_epi64(mask_bytes2);
                __m256i data2 = _mm256_loadu_si256((__m256i*)&data_row[j+4]);
                __m256i masked2 = _mm256_and_si256(data2, mask_64_2);
                
                // Accumulate both
                acc = _mm256_xor_si256(acc, masked1);
                acc = _mm256_xor_si256(acc, masked2);
            }
            
            // Horizontal XOR reduction
            __m128i acc_low = _mm256_castsi256_si128(acc);
            __m128i acc_high = _mm256_extracti128_si256(acc, 1);
            acc_low = _mm_xor_si128(acc_low, acc_high);
            
            uint64_t tmp[2];
            _mm_storeu_si128((__m128i*)tmp, acc_low);
            result = tmp[0] ^ tmp[1];
            
            // Remainder loop (branchless)
            for (; j < SIZE; j++) {
                result ^= data_row[j] & (-(TYPE_DATA)mask[j]);
            }
        } else {
            // Fallback: 4-way unrolled branchless
            int j = 0;
            for (; j + 3 < SIZE; j += 4) {
                result ^= data_row[j+0] & (-(TYPE_DATA)mask[j+0]);
                result ^= data_row[j+1] & (-(TYPE_DATA)mask[j+1]);
                result ^= data_row[j+2] & (-(TYPE_DATA)mask[j+2]);
                result ^= data_row[j+3] & (-(TYPE_DATA)mask[j+3]);
            }
            for (; j < SIZE; j++) {
                result ^= data_row[j] & (-(TYPE_DATA)mask[j]);
            }
        }
        
        opt->dot_product_output[i] = result;
    }
    
    pthread_exit((void*)opt);
}

void* ServerDuetORAM::thread_xorProduct_func(void* args)
{
    THREAD_COMPUTATION* opt = (THREAD_COMPUTATION*) args;
    const int SIZE = BUCKET_SIZE * (H + 2);

    for (int i = opt->start; i < opt->end; i++)
    {
        TYPE_DATA* __restrict__ dst = opt->dot_product_vector_xored[i];
        TYPE_DATA* __restrict__ src = opt->dot_product_vector_xored_in[i];
        
        int j = 0;
        
        if (CPUFeatures::has_avx2()) {
            // Vectorized XOR: 4×64-bit per iteration
            for (; j + 3 < SIZE; j += 4) {
                __m256i a = _mm256_loadu_si256((__m256i*)&dst[j]);
                __m256i b = _mm256_loadu_si256((__m256i*)&src[j]);
                __m256i result = _mm256_xor_si256(a, b);
                _mm256_storeu_si256((__m256i*)&dst[j], result);
            }
        }
        
        // Handle remainder
        for (; j < SIZE; j++) {
            dst[j] = dst[j] ^ src[j];
        }
    }
    
    pthread_exit((void*)opt);
}

int ServerDuetORAM::recvBlock(zmq::socket_t& socket)
{
    cout<< "	[recvBlock] Receiving Block Data..." <<endl;
	auto start = time_now;
    // COMMUNICATION:
	socket.recv(block_buffer_in, sizeof(TYPE_DATA)*DATA_CHUNKS+sizeof(TYPE_INDEX)+sizeof(block), 0);
    auto end = time_now;
    TYPE_INDEX slotIdx;
    TYPE_DATA recvBlock[DATA_CHUNKS];
    block recv_iv;
    memcpy(recvBlock, &block_buffer_in[0], sizeof(TYPE_DATA)*DATA_CHUNKS);
    memcpy(&slotIdx, &block_buffer_in[sizeof(TYPE_DATA)*DATA_CHUNKS], sizeof(TYPE_INDEX));
    memcpy(&recv_iv, &block_buffer_in[sizeof(TYPE_INDEX)+sizeof(TYPE_DATA)*DATA_CHUNKS], sizeof(block));

    cout<< "	[recvBlock] Block Data RECV in " << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count() <<endl;
    server_logs[4] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();

    start = time_now;

    // Update root bucket
    FILE *file_update;
    string path = rootPath + to_string(this->serverNo) + "/0";

    if((file_update = fopen(path.c_str(),"r+b")) == NULL)
    {
        cout<< "	[recvBlock] File Cannot be Opened!!" <<endl;
        exit(0);
    }
    fseek(file_update, slotIdx*sizeof(TYPE_DATA), SEEK_SET);
    for (int u = 0; u < DATA_CHUNKS; u++)
    {
        fwrite(&recvBlock[u], 1, sizeof(TYPE_DATA), file_update);
        fseek(file_update, (BUCKET_SIZE -1)*sizeof(TYPE_DATA), SEEK_CUR);
    }
    fclose(file_update);

    FILE* file_update_iv;
    string path_iv = rootPath + "client_iv" + to_string(this->serverNo+1) + "/0";
    if((file_update_iv = fopen(path_iv.c_str(),"r+b")) == NULL)
    {
        cout<< "	[recvBlock] File Cannot be Opened!!" <<endl;
        exit(0);
    }

    fseek(file_update_iv, slotIdx*sizeof(block), SEEK_SET);
    fwrite(&recv_iv, 1, sizeof(block), file_update_iv);
    fclose(file_update_iv);

    end = time_now;
	cout<< "	[recvBlock] Block STORED in Disk in " << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count() <<endl;
	server_logs[5] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();
	
    socket.send((unsigned char*)CMD_SUCCESS,sizeof(CMD_SUCCESS));
	cout<< "	[recvBlock] ACK is SENT!" <<endl;

    return 0;
}

int ServerDuetORAM::recvInitialPermutation(zmq::socket_t& socket)
{
    int ret = 1;
    // COMMUNICATION:
    socket.recv(key_permutation_buffer_in, sizeof(block)*(1+ (H+2)*BUCKET_SIZE * (H+2)*BUCKET_SIZE),0);
    socket.send((unsigned char*)CMD_SUCCESS, sizeof(CMD_SUCCESS), 0);
    cout<< "	[recvInitialPermutation] ACK is SENT!" <<endl;

    memcpy(&this->keytoPermutation, &key_permutation_buffer_in[0], sizeof(block));
    memcpy(this->receivedPuncturedPermutation, &key_permutation_buffer_in[1], sizeof(block)*((H+2)*BUCKET_SIZE * (H+2)*BUCKET_SIZE));
    cout<< "	[recvInitialPermutation] Sparsing key_permutation_buffer_in into key and punctured permutation is finished!" <<endl;

    ret = 0;

    return ret;
}

int ServerDuetORAM::generatePermutationOffline(const block& keytoPermutation, block* fullPermutation)
{
    PRG prg;
    prg.reseed(&keytoPermutation);
    prg.random_block(fullPermutation, (H+2)*BUCKET_SIZE * (H+2)*BUCKET_SIZE);

    // #pragma omp parallel
    // {
    //     PRG prg_local;
    //     TYPE_DATA rand_data[DATA_CHUNKS];
        
    //     #pragma omp for schedule(static)
    //     for (int i = 0; i < SIZE_PI * SIZE_PI; i++)
    //     {
    //         prg_local.reseed(&fullPermutation[i]);
    //         prg_local.random_data_unaligned(rand_data, sizeof(TYPE_DATA)*DATA_CHUNKS);
    //         for (int j = 0; j < DATA_CHUNKS; j++)
    //         {
    //             fullPermutationExpansion[j][i] = rand_data[j];
    //         }

    //         prg_local.reseed(&receivedPuncturedPermutation[i]);
    //         prg_local.random_data_unaligned(rand_data, sizeof(TYPE_DATA)*DATA_CHUNKS);
    //         for (int j = 0; j < DATA_CHUNKS; j++)
    //         {
    //             puncturedPermutationExpansion[j][i] = rand_data[j];
    //         }
    //     }
    // }

    return 0;
}

int ServerDuetORAM::evict(zmq::socket_t& socket)
{
    DuetORAM ORAM;
    SecretSharedShuffle sss;
    TYPE_INDEX evict_pathID;
    PRG prg;

    cout<< "	[evict] Receiving Evict Information..." <<endl;;
	auto start = time_now;
    // COMMUNICATION:
    socket.recv(evict_buffer_in, sizeof(TYPE_INDEX) + sizeof(block) + 3 * sizeof(TYPE_INDEX) * SIZE_PI,0);
    auto end = time_now;
    cout<< "	[evict] RECEIVED! in " << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count() <<endl;
	server_logs[6] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();

    start = time_now;
    memcpy(&evict_pathID, &evict_buffer_in[0], sizeof(TYPE_INDEX));
    memcpy(&this->seed_iv, &evict_buffer_in[sizeof(TYPE_INDEX)], sizeof(block));
    memcpy(this->sub_pi, &evict_buffer_in[sizeof(TYPE_INDEX)+sizeof(block)], sizeof(TYPE_INDEX)*SIZE_PI);
    memcpy(this->circularShift_1, &evict_buffer_in[sizeof(TYPE_INDEX)+sizeof(block)+sizeof(TYPE_INDEX)*SIZE_PI], sizeof(TYPE_INDEX)*SIZE_PI);
    memcpy(this->circularShift_2, &evict_buffer_in[sizeof(TYPE_INDEX)+sizeof(block)+2*sizeof(TYPE_INDEX)*SIZE_PI], sizeof(TYPE_INDEX)*SIZE_PI);

    TYPE_INDEX fullPathIdx[H+2];
    ORAM.getFullPathIdx(fullPathIdx, evict_pathID);
    fullPathIdx[H+1] = sss.getSibling(evict_pathID);

    // 1. use thread to load data from files
    
    int step = ceil((double)DATA_CHUNKS/(double)numThreads);
    int endIdx;

    THREAD_LOADDATA loadData_args[numThreads];
    TYPE_DATA** evict_vector = new TYPE_DATA*[DATA_CHUNKS];
    for (TYPE_INDEX k = 0; k < DATA_CHUNKS; k++)
    {
        evict_vector[k] = new TYPE_DATA[BUCKET_SIZE*(H+2)];
    }
    
    for(int i = 0, startIdx = 0; i < numThreads , startIdx < DATA_CHUNKS; i ++, startIdx+=step)
    {
        if(startIdx+step > DATA_CHUNKS)
            endIdx = DATA_CHUNKS;
        else
            endIdx = startIdx+step;
        
        loadData_args[i] = THREAD_LOADDATA(this->serverNo, startIdx, endIdx, evict_vector, fullPathIdx, H+2);
        pthread_create(&thread_compute[i], NULL, &ServerDuetORAM::thread_loadRetrievalData_func, (void*)&loadData_args[i]);

        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i, &cpuset);
        pthread_setaffinity_np(thread_compute[i], sizeof(cpu_set_t), &cpuset);
    }
    for(int i = 0; i < numThreads; i++)
    {
        pthread_join(thread_compute[i],NULL);
    }

    end = time_now;
	long load_time = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();
    server_logs[7] = load_time;
    cout<< "	[evict] Path Nodes READ from Disk in " << load_time << " ns"<<endl;

    // 2. read iv from files
    start = time_now;
    block* ivs = new block[BUCKET_SIZE*(H+2)];
    FILE* file_iv = NULL;
    string path_iv;
    for (int i = 0; i < H+2; i++)
    {
        file_iv = NULL;
        path_iv = rootPath + "client_iv" + to_string(this->serverNo+1) + "/" + to_string(fullPathIdx[i]);
        if((file_iv = fopen(path_iv.c_str(),"rb")) == NULL){
            cout<< "	[evict] File cannot be opened!!" <<endl;
            exit(0);
        }
        long lSize_iv;
        fseek(file_iv, 0, SEEK_END);
        lSize_iv = ftell(file_iv);
        rewind(file_iv);
        if(fread(&ivs[i*BUCKET_SIZE],1 , BUCKET_SIZE*sizeof(block), file_iv) != sizeof(char)*lSize_iv){
            cout<< "	[evict] File loading error be Read!!" <<endl;
            exit(0);
        }
        fclose(file_iv);
    }
    end = time_now;
    server_logs[8] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();
    
    start = time_now;
    TYPE_DATA** keystream = new TYPE_DATA* [BUCKET_SIZE*(H+2)];
    for (int i = 0 ; i < BUCKET_SIZE*(H+2); i++)
    {
        keystream[i] = new TYPE_DATA[DATA_CHUNKS];
    }
    
    // AES-NI hardware-accelerated keystream generation (10-20× faster than software AES)
    if (CPUFeatures::has_aesni()) {
        // Hardware path: Use AES-NI intrinsics
        AESNIWrapper aes_engine;
        aes_engine.set_encrypt_key(reinterpret_cast<const unsigned char*>(&this->mask_key));
        
        #pragma omp parallel for schedule(static)
        for (int i = 0 ; i < BUCKET_SIZE*(H+2); i++)
        {
            aes_engine.generate_keystream_ctr<TYPE_DATA>(
                reinterpret_cast<const unsigned char*>(&this->mask_key),
                reinterpret_cast<const unsigned char*>(&ivs[i]),
                keystream[i],
                sizeof(TYPE_DATA)*DATA_CHUNKS
            );
        }
    } else {
        // Software fallback: Original emp-tool implementation
        #pragma omp parallel for schedule(static)
        for (int i = 0 ; i < BUCKET_SIZE*(H+2); i++)
        {
            memset(keystream[i],0,sizeof(TYPE_DATA)*DATA_CHUNKS);
            aes_128_ctr<TYPE_DATA>(this->mask_key, ivs[i], nullptr, (uint8_t*)keystream[i], sizeof(TYPE_DATA)*DATA_CHUNKS);
        }
    }
    end = time_now;
    server_logs[9] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();

    start = time_now;
    TYPE_DATA** evict_vector_for_shuffle = new TYPE_DATA*[DATA_CHUNKS];
    for(TYPE_INDEX i = 0; i < DATA_CHUNKS; i++)
    {
        evict_vector_for_shuffle[i] = new TYPE_DATA[BUCKET_SIZE*(H+2)];
        memset(evict_vector_for_shuffle[i],0,sizeof(TYPE_DATA)*BUCKET_SIZE*(H+2));
    }


    if (this->serverNo == 0)
    {
        transpose_parallel(keystream, evict_vector_for_shuffle, BUCKET_SIZE*(H+2), DATA_CHUNKS);
    }
    if (this->serverNo == 1)
    {
        transpose_parallel(keystream, evict_vector_for_shuffle, BUCKET_SIZE*(H+2), DATA_CHUNKS);
        xor_vectors_optimized(evict_vector_for_shuffle, evict_vector, DATA_CHUNKS, BUCKET_SIZE*(H+2));
    }
    end = time_now;
    server_logs[10] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();
    
    
    // 5. Circular Shift
    start = time_now;
    efficient_rotate();
    end = time_now;
    server_logs[11] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();
    
    // 6. Expansion (optimized with OpenMP parallelization)
    // Note: Each thread needs its own PRG instance to avoid race conditions
    // #pragma omp parallel
    // {
    //     PRG prg_local;
    //     TYPE_DATA rand_data[DATA_CHUNKS];
        
    //     #pragma omp for schedule(static)
    //     for (int i = 0; i < SIZE_PI * SIZE_PI; i++)
    //     {
    //         prg_local.reseed(&fullPermutation[i]);
    //         prg_local.random_data_unaligned(rand_data, sizeof(TYPE_DATA)*DATA_CHUNKS);
    //         for (int j = 0; j < DATA_CHUNKS; j++)
    //         {
    //             fullPermutationExpansion[j][i] = rand_data[j];
    //         }

    //         prg_local.reseed(&receivedPuncturedPermutation[i]);
    //         prg_local.random_data_unaligned(rand_data, sizeof(TYPE_DATA)*DATA_CHUNKS);
    //         for (int j = 0; j < DATA_CHUNKS; j++)
    //         {
    //             puncturedPermutationExpansion[j][i] = rand_data[j];
    //         }
    //     }
    // }

    //FIXME: Clear a, b, delta before computation to avoid using old values from previous eviction
    for (int i = 0; i < DATA_CHUNKS; i++)
    {
        memset(this->a[i], 0, sizeof(TYPE_DATA)*SIZE_PI);
        memset(this->b[i], 0, sizeof(TYPE_DATA)*SIZE_PI);
        memset(this->delta[i], 0, sizeof(TYPE_DATA)*SIZE_PI);
    }

    // ============================================================
    // 6.1 compute a
    // Outer loop: column j → accesses fullPermutation[j + i*SIZE_PI]
    // Access pattern: stride SIZE_PI (column-major), each thread owns its j.
    // Stack-only temporaries → zero heap pressure inside the parallel region.
    // ============================================================
    start = time_now;

    #pragma omp parallel for schedule(static)
    for (TYPE_INDEX j = 0; j < SIZE_PI; j++)
    {
        PRG prg_a;
        TYPE_DATA result[DATA_CHUNKS] = {0};  // thread-local accumulator
        TYPE_DATA expanded[DATA_CHUNKS];       // single-block scratch buffer

        for (TYPE_INDEX i = 0; i < SIZE_PI; i++)
        {
            // Access pattern j + i*SIZE_PI is strided but consistent per thread;
            // hardware prefetcher trains on it quickly within each thread's slice.
            prg_a.reseed(&fullPermutation[j + i*SIZE_PI]);
            prg_a.random_data_unaligned(expanded, sizeof(TYPE_DATA)*DATA_CHUNKS);

            #pragma omp simd
            for (TYPE_INDEX k = 0; k < DATA_CHUNKS; k++)
                result[k] ^= expanded[k];
        }

        // Write all DATA_CHUNKS values in one contiguous burst per column j.
        // this->a[k][j]: each k-row is a separate array, no false sharing here.
        for (TYPE_INDEX k = 0; k < DATA_CHUNKS; k++)
            this->a[k][j] = result[k];
    }

    end = time_now;
    server_logs[12] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();
    cout << "Time for computing a: " << server_logs[12] << " ns" << endl;


    // ============================================================
    // 6.2 compute b
    // Outer loop: row i → accesses fullPermutation[i*SIZE_PI + j]
    // Access pattern: fully sequential (row-major). Ideal for prefetcher.
    // ============================================================
    start = time_now;

    #pragma omp parallel for schedule(static)
    for (TYPE_INDEX i = 0; i < SIZE_PI; i++)
    {
        PRG prg_b;
        TYPE_DATA result[DATA_CHUNKS] = {0};
        TYPE_DATA expanded[DATA_CHUNKS];

        for (TYPE_INDEX j = 0; j < SIZE_PI; j++)
        {
            // Sequential row-major access: best possible cache locality.
            prg_b.reseed(&fullPermutation[i*SIZE_PI + j]);
            prg_b.random_data_unaligned(expanded, sizeof(TYPE_DATA)*DATA_CHUNKS);

            #pragma omp simd
            for (TYPE_INDEX k = 0; k < DATA_CHUNKS; k++)
                result[k] ^= expanded[k];
        }

        for (TYPE_INDEX k = 0; k < DATA_CHUNKS; k++)
            this->b[k][i] = result[k];
    }

    end = time_now;
    server_logs[13] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();
    cout << "Time for computing b: " << server_logs[13] << " ns" << endl;


    // ============================================================
    // 6.3 compute delta
    //
    // delta[k][i] = XOR_j(expand(punctured[i*SIZE_PI + j]))   [row i]
    //             ^ XOR_j(expand(punctured[j*SIZE_PI + sub_pi[i]]))  [col sub_pi[i]]
    //
    // OPTIMIZATION STRATEGY - zero temporary 2D matrices:
    //
    //   Stage 1 (rows, row-major access):
    //     Write directly into this->delta[k][i].
    //
    //   Stage 2 (columns, sequential col access via inv_sub_pi):
    //     Build a 1-to-1 inverse map: inv_sub_pi[sub_pi[i]] = i  (bijection → no races)
    //     Loop over col = 0..SIZE_PI-1 sequentially → row-major-like strides on
    //     punctured[j*SIZE_PI + col], which the prefetcher handles well.
    //     XOR column result directly into this->delta[k][inv_sub_pi[col]].
    //
    //   No delta_row / delta_col matrices needed → eliminates SIZE_PI×DATA_CHUNKS
    //   heap allocations and the O(N) new/delete loop entirely.
    //
    //   Allocate inv_sub_pi BEFORE timing starts; free it AFTER timing ends.
    // ============================================================

    // PRE-TIMING: Build inverse permutation (1D array, no nested heap).
    // inv_sub_pi[col] = unique row i such that sub_pi[i] == col.
    TYPE_INDEX* inv_sub_pi = new TYPE_INDEX[SIZE_PI];
    for (TYPE_INDEX i = 0; i < SIZE_PI; i++)
        inv_sub_pi[sub_pi[i]] = i;

    start = time_now;

    // --- Stage 1: Row contributions (sequential row-major access) ---
    // Identical access pattern to 'b'. Write directly into this->delta.
    #pragma omp parallel for schedule(static)
    for (TYPE_INDEX i = 0; i < SIZE_PI; i++)
    {
        PRG prg_row;
        TYPE_DATA result[DATA_CHUNKS] = {0};
        TYPE_DATA expanded[DATA_CHUNKS];

        for (TYPE_INDEX j = 0; j < SIZE_PI; j++)
        {
            prg_row.reseed(&receivedPuncturedPermutation[i*SIZE_PI + j]);
            prg_row.random_data_unaligned(expanded, sizeof(TYPE_DATA)*DATA_CHUNKS);

            #pragma omp simd
            for (TYPE_INDEX k = 0; k < DATA_CHUNKS; k++)
                result[k] ^= expanded[k];
        }

        // Direct write: each thread owns a distinct i → no false sharing.
        for (TYPE_INDEX k = 0; k < DATA_CHUNKS; k++)
            this->delta[k][i] = result[k];
    }

    // --- Stage 2: Column contributions (sequential column access) ---
    // Loop variable is 'col', NOT 'i', so access punctured[j*SIZE_PI + col]
    // is stride-SIZE_PI but COL is monotonically increasing across the loop,
    // which means adjacent OpenMP threads work on adjacent columns → good
    // spatial locality across the thread team.
    // The bijection inv_sub_pi[col] = i guarantees no two threads write to
    // the same this->delta[k][row_i] → no race conditions.
    #pragma omp parallel for schedule(static)
    for (TYPE_INDEX col = 0; col < SIZE_PI; col++)
    {
        PRG prg_col;
        TYPE_DATA col_result[DATA_CHUNKS] = {0};
        TYPE_DATA expanded[DATA_CHUNKS];

        for (TYPE_INDEX j = 0; j < SIZE_PI; j++)
        {
            // Sequential over col: each thread's assigned cols are contiguous.
            // Hardware prefetcher easily tracks punctured[j*SIZE_PI + col].
            prg_col.reseed(&receivedPuncturedPermutation[j*SIZE_PI + col]);
            prg_col.random_data_unaligned(expanded, sizeof(TYPE_DATA)*DATA_CHUNKS);

            #pragma omp simd
            for (TYPE_INDEX k = 0; k < DATA_CHUNKS; k++)
                col_result[k] ^= expanded[k];
        }

        // Bijection: exactly one row_i maps to this col → zero race conditions.
        // XOR directly into this->delta, eliminating delta_col entirely.
        TYPE_INDEX row_i = inv_sub_pi[col];
        for (TYPE_INDEX k = 0; k < DATA_CHUNKS; k++)
            this->delta[k][row_i] ^= col_result[k];
    }

    end = time_now;

    // POST-TIMING: free the single 1D helper array.
    delete[] inv_sub_pi;

    server_logs[14] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();
    cout << "Time for computing delta: " << server_logs[14] << " ns" << endl;
    
    
    // 7. secret shared shuffle
    start = time_now;
    TYPE_DATA** resultShare = new TYPE_DATA*[DATA_CHUNKS];
    for (int i = 0; i < DATA_CHUNKS; i++)
    {
        resultShare[i] = new TYPE_DATA[SIZE_PI];
        memset(resultShare[i],0,sizeof(TYPE_DATA)*SIZE_PI);
    }
    
    sss.secretShare(this->serverNo, numThreads, this->sub_pi, evict_vector_for_shuffle, this->delta, this->a, this->b, resultShare);
    end = time_now;
    server_logs[15] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();

    start = time_now;
    block* list_iv = new block[SIZE_PI];
    prg.reseed(&this->seed_iv);
    prg.random_block(list_iv, SIZE_PI);
    cout << "    [evict] Server-" << this->serverNo << " generated " << SIZE_PI << " new IVs from seed: " << this->seed_iv << endl;
    cout << "    [evict] First new IV: " << list_iv[0] << ", Last new IV: " << list_iv[SIZE_PI-1] << endl;


    TYPE_DATA** dot_product_vector_xored_in = new TYPE_DATA*[DATA_CHUNKS]; 
    TYPE_DATA** dot_product_vector_xored = new TYPE_DATA*[DATA_CHUNKS];
    for (TYPE_INDEX i = 0; i < DATA_CHUNKS; i++)
    {
        dot_product_vector_xored[i] = new TYPE_DATA[SIZE_PI];
        dot_product_vector_xored_in[i] = new TYPE_DATA[SIZE_PI];
        memset(dot_product_vector_xored[i],0,sizeof(TYPE_DATA)*SIZE_PI);
        memset(dot_product_vector_xored_in[i],0,sizeof(TYPE_DATA)*SIZE_PI);
    }


    TYPE_DATA ** key_stream_for_exchange = new TYPE_DATA*[SIZE_PI];
    for (TYPE_INDEX i = 0; i < SIZE_PI; i++)
    {
        key_stream_for_exchange[i] = new TYPE_DATA[DATA_CHUNKS];
    }
    
    // AES-NI hardware-accelerated keystream generation
    if (CPUFeatures::has_aesni()) {
        // Hardware path: Use AES-NI intrinsics (10-20× faster)
        AESNIWrapper aes_engine;
        aes_engine.set_encrypt_key(reinterpret_cast<const unsigned char*>(&this->mask_key));
        
        #pragma omp parallel for schedule(static)
        for (TYPE_INDEX i = 0; i < SIZE_PI; i++)
        {
            memset(key_stream_for_exchange[i],0,sizeof(TYPE_DATA)*DATA_CHUNKS);
            aes_engine.generate_keystream_ctr<TYPE_DATA>(
                reinterpret_cast<const unsigned char*>(&this->mask_key),
                reinterpret_cast<const unsigned char*>(&list_iv[i]),
                key_stream_for_exchange[i],
                sizeof(TYPE_DATA)*DATA_CHUNKS
            );
        }
    } else {
        // Software fallback: Original emp-tool implementation
        #pragma omp parallel for schedule(static)
        for (TYPE_INDEX i = 0; i < SIZE_PI; i++)
        {
            memset(key_stream_for_exchange[i],0,sizeof(TYPE_DATA)*DATA_CHUNKS);
            aes_128_ctr<TYPE_DATA>(this->mask_key, list_iv[i], nullptr, (uint8_t*)key_stream_for_exchange[i], sizeof(TYPE_DATA)*DATA_CHUNKS);
        }
    }

    #pragma omp parallel for schedule(static)
    for (TYPE_INDEX i = 0; i < DATA_CHUNKS; i++)
    {
        TYPE_DATA* result_row = resultShare[i];
        TYPE_DATA* xored_row = dot_product_vector_xored[i];
        for (TYPE_INDEX j = 0; j < SIZE_PI; j++)
        {
            xored_row[j] = result_row[j] ^ key_stream_for_exchange[j][i];
        }
    }
    end = time_now;
    server_logs[16] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();

    // 9. distribute xored dot_product_vector to another server
    start = time_now;

    //===========FOR LISTENING=======================================================
    struct_socket recvSocket_arg;
    cout << "   [server listens dot product vector] Creating Threads for Receiving vector..." << endl;
    recvSocket_arg = struct_socket("tcp://*:" + std::to_string(SERVER_PORT+(serverNo)*(NUM_SERVERS)+this->others), NULL, 0, this->path_vector_in_for_identical_copy, DATA_CHUNKS * SIZE_PI * sizeof(TYPE_DATA), NULL, false);
    pthread_create(&thread_recv, NULL, &ServerDuetORAM::thread_socket_func, (void*)&recvSocket_arg);
    cout << "   [server listens dot product vector] Received xored dot product vector!" << endl;
    //===============================================================================

    //===========FOR SENDING=========================================================
    struct_socket sendSocket_arg;
    cout << "   [server sends dot product vector] Creating Threads for Sending vector..." << endl;
    for (int i = 0; i < DATA_CHUNKS; i++)
    {
        memcpy(&this->path_vector_out_for_identical_copy[i*SIZE_PI*sizeof(TYPE_DATA)], dot_product_vector_xored[i], SIZE_PI*sizeof(TYPE_DATA));
    }
    
    sendSocket_arg = struct_socket("tcp://localhost:" + to_string(SERVER_PORT+this->others*NUM_SERVERS+this->serverNo), this->path_vector_out_for_identical_copy, DATA_CHUNKS*SIZE_PI*sizeof(TYPE_DATA), NULL, 0, NULL, true);
    pthread_create(&thread_send, NULL, &ServerDuetORAM::thread_socket_func, (void*)&sendSocket_arg);
    cout << "   [server sends dot product vector] Sent xored dot product vector!" << endl;
    //===============================================================================

    cout << "   [server] Waiting for Threads..." << endl;
    pthread_join(thread_send, NULL);
    pthread_join(thread_recv, NULL);

    cout<< "	[server] DONE!" <<endl;
	server_logs[17] = thread_max;
	thread_max = 0;
    end = time_now;
    server_logs[18] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();
    cout<< "	[EVICT] Xored Dot Product Vector SENT and RECV in " << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count() << " ns"<<endl;

    cout << "   [server] Sparsing received xored dot product vector" << endl;
    for (int i = 0; i < DATA_CHUNKS; i++)
    {
        memcpy(dot_product_vector_xored_in[i], &path_vector_in_for_identical_copy[i*SIZE_PI*sizeof(TYPE_DATA)], SIZE_PI*sizeof(TYPE_DATA));
    }
    cout << "   [server] Sparsed!" << endl;

    // Multithread for dot product computation
    start = time_now;
    THREAD_COMPUTATION dotProduct_args[numThreads];
    endIdx = 0;
    step = ceil((double)DATA_CHUNKS/(double)numThreads);
    for(int i = 0, startIdx = 0 ; i < numThreads , startIdx < DATA_CHUNKS; i ++, startIdx+=step)
    {
        if (startIdx + step > DATA_CHUNKS)
            endIdx = DATA_CHUNKS;
        else
            endIdx = startIdx + step;
        
        dotProduct_args[i] = THREAD_COMPUTATION(startIdx, endIdx, dot_product_vector_xored, dot_product_vector_xored_in);
        pthread_create(&thread_compute[i], NULL, &ServerDuetORAM::thread_xorProduct_func, (void*)&dotProduct_args[i]);

        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i, &cpuset);
        pthread_setaffinity_np(thread_compute[i], sizeof(cpu_set_t), &cpuset);
    }

    for(int i = 0, startIdx = 0 ; i < numThreads , startIdx < DATA_CHUNKS; i ++, startIdx+=step)
    {
        pthread_join(thread_compute[i],NULL);
    }

    end = time_now;
    server_logs[19] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();
    cout<< "	[EVICT] GET IDENTICAL PATH CALCULATED in " << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count() <<endl;

    // 10. write back to files
    start = time_now;
    FILE* file_out_block = NULL;
    string path_out_block;
    FILE* file_out_iv = NULL;
    string path_out_iv;
    TYPE_INDEX num_bucket;

    cout << "    [evict] Writing back shuffled data to disk..." << endl;
    for (int l = 0; l < H; l++)
    {
        num_bucket = sss.getSibling(fullPathIdx[l+1]);
        // cout << "    [evict] Layer " << l << ": Writing to bucket " << num_bucket << " (sibling of " << fullPathIdx[l+1] << ")" << endl;
        path_out_block = rootPath + to_string(this->serverNo) + "/" + to_string(num_bucket);
        // cout << "    [evict] Layer " << l << ": Writing to bucket " << num_bucket << " (sibling of " << fullPathIdx[l+1] << ")" << endl;
        path_out_iv = rootPath + "client_iv" + to_string(this->serverNo+1) + "/" + to_string(num_bucket);
        if((file_out_block = fopen(path_out_block.c_str(),"wb+")) == NULL)
        {
            cout<< "	[evict] File Cannot be Opened!!" <<endl;
            exit(0);
        }
        for (int i = 0; i < DATA_CHUNKS; i++)
        {
            fwrite(&dot_product_vector_xored[i][l*BUCKET_SIZE], 1, sizeof(TYPE_DATA)*BUCKET_SIZE, file_out_block);
        }
        fclose(file_out_block);

        if((file_out_iv = fopen(path_out_iv.c_str(),"wb+")) == NULL)
        {
            cout<< "	[evict] File Cannot be Opened!!" <<endl;
            exit(0);
        }
        // write newly generated IVs for the permuted slots
        fwrite(&list_iv[l*BUCKET_SIZE], 1, sizeof(block)*BUCKET_SIZE, file_out_iv);
        fclose(file_out_iv);
    }

    num_bucket = fullPathIdx[H];
    path_out_block = rootPath + to_string(this->serverNo) + "/" + to_string(num_bucket);
    // cout << "    [evict] Leaf (layer " << H << "): Writing to bucket " << num_bucket << endl;
    path_out_iv = rootPath + "client_iv" + to_string(this->serverNo+1) + "/" + to_string(num_bucket);
    if((file_out_block = fopen(path_out_block.c_str(),"wb+")) == NULL)
    {
        cout<< "	[evict] File Cannot be Opened!!" <<endl;
        exit(0);
    }
    for (int i = 0; i < DATA_CHUNKS; i++)
    {
        fwrite(&dot_product_vector_xored[i][H*BUCKET_SIZE], 1, sizeof(TYPE_DATA)*BUCKET_SIZE, file_out_block);
    }
    fclose(file_out_block);
    if((file_out_iv = fopen(path_out_iv.c_str(),"wb+")) == NULL)
    {
        cout<< "	[evict] File Cannot be Opened!!" <<endl;
        exit(0);
    }
    // write new IVs for the leaf bucket
    fwrite(&list_iv[H*BUCKET_SIZE], 1, sizeof(block)*BUCKET_SIZE, file_out_iv);
    fclose(file_out_iv);

    num_bucket = fullPathIdx[0];
    path_out_block = rootPath + to_string(this->serverNo) + "/" + to_string(num_bucket);
    cout << "    [evict] Root (layer 0): Writing to bucket " << num_bucket << " using shuffle layer " << (H+1) << endl;
    path_out_iv = rootPath + "client_iv" + to_string(this->serverNo+1) + "/" + to_string(num_bucket);
    if((file_out_block = fopen(path_out_block.c_str(),"wb+")) == NULL)
    {
        cout<< "	[evict] File Cannot be Opened!!" <<endl;
        exit(0);
    }
    for (int i = 0; i < DATA_CHUNKS; i++)
    {
        fwrite(&dot_product_vector_xored[i][(H+1)*BUCKET_SIZE], 1, sizeof(TYPE_DATA)*BUCKET_SIZE, file_out_block);
    }
    fclose(file_out_block);
    if((file_out_iv = fopen(path_out_iv.c_str(),"wb+")) == NULL)
    {
        cout<< "	[evict] File Cannot be Opened!!" <<endl;
        exit(0);
    }
    // write new IVs for the root bucket (from shuffle layer H+1)
    fwrite(&list_iv[(H+1)*BUCKET_SIZE], 1, sizeof(block)*BUCKET_SIZE, file_out_iv);
    fclose(file_out_iv);
    
    end = time_now;
    server_logs[20] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();
    cout<< "	[SendBlock] Eviction time in " << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count() <<endl;

    socket.send((unsigned char*)CMD_SUCCESS,sizeof(CMD_SUCCESS));
	cout<< "	[evict] ACK is SENT!" <<endl;

    return 0;
}

// Helper: AVX2 4×4 64-bit matrix transpose
static inline void __attribute__((always_inline)) transpose_4x4_epi64(
    __m256i &r0, __m256i &r1, __m256i &r2, __m256i &r3)
{
    __m256i t0 = _mm256_unpacklo_epi64(r0, r1);
    __m256i t1 = _mm256_unpackhi_epi64(r0, r1);
    __m256i t2 = _mm256_unpacklo_epi64(r2, r3);
    __m256i t3 = _mm256_unpackhi_epi64(r2, r3);
    
    r0 = _mm256_permute2x128_si256(t0, t2, 0x20);
    r1 = _mm256_permute2x128_si256(t1, t3, 0x20);
    r2 = _mm256_permute2x128_si256(t0, t2, 0x31);
    r3 = _mm256_permute2x128_si256(t1, t3, 0x31);
}

void ServerDuetORAM::transpose_parallel(TYPE_DATA** __restrict__ src, 
                                         TYPE_DATA** __restrict__ dst, 
                                         int R, int C)
{
    // Optimal cache block size for i7-1360P L1D cache (48KB)
    // 48KB / 8 bytes = 6144 elements → sqrt ≈ 78, round down to 64
    const int CACHE_BLOCK_SIZE = 64;
    
    #pragma omp parallel for collapse(2) schedule(dynamic, 1)
    for (int ii = 0; ii < R; ii += CACHE_BLOCK_SIZE) {
        for (int jj = 0; jj < C; jj += CACHE_BLOCK_SIZE) {
            
            int i_end = std::min(ii + CACHE_BLOCK_SIZE, R);
            int j_end = std::min(jj + CACHE_BLOCK_SIZE, C);
            
            if (CPUFeatures::has_avx2()) {
                // AVX2 path: Process 4×4 blocks
                int i = ii;
                for (; i + 3 < i_end; i += 4) {
                    int j = jj;
                    for (; j + 3 < j_end; j += 4) {
                        __m256i r0 = _mm256_loadu_si256((__m256i*)&src[i+0][j]);
                        __m256i r1 = _mm256_loadu_si256((__m256i*)&src[i+1][j]);
                        __m256i r2 = _mm256_loadu_si256((__m256i*)&src[i+2][j]);
                        __m256i r3 = _mm256_loadu_si256((__m256i*)&src[i+3][j]);
                        
                        transpose_4x4_epi64(r0, r1, r2, r3);
                        
                        _mm256_storeu_si256((__m256i*)&dst[j+0][i], r0);
                        _mm256_storeu_si256((__m256i*)&dst[j+1][i], r1);
                        _mm256_storeu_si256((__m256i*)&dst[j+2][i], r2);
                        _mm256_storeu_si256((__m256i*)&dst[j+3][i], r3);
                    }
                    
                    // Handle j remainder
                    for (; j < j_end; j++) {
                        dst[j][i+0] = src[i+0][j];
                        dst[j][i+1] = src[i+1][j];
                        dst[j][i+2] = src[i+2][j];
                        dst[j][i+3] = src[i+3][j];
                    }
                }
                
                // Handle i remainder
                for (; i < i_end; i++) {
                    for (int j = jj; j < j_end; j++) {
                        dst[j][i] = src[i][j];
                    }
                }
            } else {
                // Scalar fallback with 4×4 blocking
                for (int i = ii; i < i_end; i += 4) {
                    for (int j = jj; j < j_end; j += 4) {
                        int i_limit = std::min(i + 4, i_end);
                        int j_limit = std::min(j + 4, j_end);
                        
                        // Manually unrolled 4×4 block
                        for (int ii2 = i; ii2 < i_limit; ii2++) {
                            for (int jj2 = j; jj2 < j_limit; jj2++) {
                                dst[jj2][ii2] = src[ii2][jj2];
                            }
                        }
                    }
                }
            }
        }
    }
}

void ServerDuetORAM::xor_vectors_optimized(TYPE_DATA** vec_a, TYPE_DATA** vec_b, int rows, int cols)
{

    #pragma omp parallel for schedule(static)
    for (int i = 0; i < rows; i++) {
        
        TYPE_DATA* row_a = vec_a[i];
        TYPE_DATA* row_b = vec_b[i];
        
        int j = 0;


        for (; j <= cols - 4; j += 4) {
            
            __m256i va = _mm256_loadu_si256((__m256i*)&row_a[j]);
            __m256i vb = _mm256_loadu_si256((__m256i*)&row_b[j]);


            __m256i res = _mm256_xor_si256(va, vb);


            _mm256_storeu_si256((__m256i*)&row_a[j], res);
        }


        for (; j < cols; j++) {
            row_a[j] = row_a[j] ^ row_b[j];
        }
    }
}


void ServerDuetORAM::efficient_rotate()
{
    // 操作块矩阵（一维），而不是扩展矩阵（二维）
    block* arr1 = (this->serverNo == 0) ? receivedPuncturedPermutation : fullPermutation;
    block* arr2 = (this->serverNo == 0) ? fullPermutation : receivedPuncturedPermutation;

    const size_t ELEMENT_SIZE = sizeof(block);
    const bool use_avx2 = CPUFeatures::has_avx2();
    
    #pragma omp parallel num_threads(numThreads)
    {
        // Thread-local temp buffer to avoid contention
        block* temp_buffer = new block[SIZE_PI];
        
        // 处理第一个矩阵 (基于 circularShift_1)
        #pragma omp for schedule(static)
        for (int i = 0; i < SIZE_PI; i++)
        {
            block* row_start = arr1 + i * SIZE_PI;
            size_t shift = circularShift_1[i];
            
            if (shift == 0 || shift == SIZE_PI) continue;
            
            // Manual rotation: right shift by 'shift' positions
            // Original: [0,1,2,3,4,5] shift=2 -> [4,5,0,1,2,3]
            
            size_t shift_bytes = shift * ELEMENT_SIZE;
            size_t remaining_bytes = (SIZE_PI - shift) * ELEMENT_SIZE;
            
            // Step 1: Copy last 'shift' elements to temp
            memcpy(temp_buffer, &row_start[SIZE_PI - shift], shift_bytes);
            
            // Step 2: Move first (SIZE_PI - shift) elements right
            memmove(&row_start[shift], &row_start[0], remaining_bytes);
            
            // Step 3: Copy temp to front
            memcpy(&row_start[0], temp_buffer, shift_bytes);
        }
        
        // 处理第二个矩阵 (基于 circularShift_2)
        #pragma omp for schedule(static)
        for (int j = 0; j < SIZE_PI; j++)
        {
            block* col_start = arr2 + j * SIZE_PI;
            size_t shift = circularShift_2[j];
            
            if (shift == 0 || shift == SIZE_PI) continue;
            
            // Same rotation logic as arr1
            size_t shift_bytes = shift * ELEMENT_SIZE;
            size_t remaining_bytes = (SIZE_PI - shift) * ELEMENT_SIZE;
            
            memcpy(temp_buffer, &col_start[SIZE_PI - shift], shift_bytes);
            memmove(&col_start[shift], &col_start[0], remaining_bytes);
            memcpy(&col_start[0], temp_buffer, shift_bytes);
        }
        
        delete[] temp_buffer;
    }
}


// void* ServerDuetORAM::thread_socket_func(void* args)
// {

//     ThreadSocketArgs* arg = (ThreadSocketArgs*)args;
//     zmq::socket_t* socket = arg->socket;
//     struct_socket* sockData = arg->sockData;
	
// 	if(sockData->isSend)
// 	{
// 		auto start = time_now;
// 		send(sockData->ADDR, sockData->data_out, sockData->data_out_size, socket);
// 		auto end = time_now;
// 		if(thread_max < std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count())
// 			thread_max = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();
// 	}
// 	else
// 	{
// 		recv(sockData->ADDR, sockData->data_in, sockData->data_in_size, socket);
// 	}
//     pthread_exit((void*)arg);
// }

int ServerDuetORAM::send(std::string ADDR, unsigned char* input, TYPE_INDEX inputSize, zmq::socket_t* socket)
{
    // zmq::context_t context(1);
    // zmq::socket_t socket(context,ZMQ_REQ);

    // socket.connect(ADDR.c_str());
	
    unsigned char buffer_in[sizeof(CMD_SUCCESS)];

    try
    {
		cout<< "	[ThreadedSocket] Sending to " << ADDR << endl;
		// socket.send (input, inputSize);
        socket->send (input, inputSize);
		cout<< "	[ThreadedSocket] Data SENT!" << ADDR << endl;
        
        // socket.recv(buffer_in, sizeof(CMD_SUCCESS));
        socket->recv(buffer_in, sizeof(CMD_SUCCESS));
        cout<< "	[ThreadedSocket] ACK RECEIVED!" << ADDR << endl;
    }
    catch (exception &ex)
    {
        goto exit;
    }

exit:
	// socket.disconnect(ADDR.c_str());
	// socket.close();
	return 0;
}

int ServerDuetORAM::recv(std::string ADDR, unsigned char* output, TYPE_INDEX outputSize, zmq::socket_t* socket)
{
    // zmq::context_t context(1);
    // zmq::socket_t socket(context,ZMQ_REP);
	
    // socket.bind(ADDR.c_str());

    try
    {
		cout<< "	[ThreadedSocket] Waiting Client on " << ADDR << endl;
		// socket.recv (output, outputSize);
        socket->recv (output, outputSize);
		cout<< "	[ThreadedSocket] Data RECEIVED! " << ADDR <<endl;
        
        // socket.send((unsigned char*)CMD_SUCCESS,sizeof(CMD_SUCCESS));
        socket->send((unsigned char*)CMD_SUCCESS,sizeof(CMD_SUCCESS));
        cout<< "	[ThreadedSocket] ACK SENT! "  << ADDR<<endl;
    }
    catch (exception &ex)
    {
        cout<<"Socket error!";
        goto exit;
    }
    
exit:
	// socket.close();
	return 0;
}

void* ServerDuetORAM::thread_socket_func(void* args)
{
    struct_socket* opt = (struct_socket*) args;

    if (opt->isSend) 
    {
        auto start = time_now;
        send(opt->ADDR, opt->data_out, opt->data_out_size);
        auto end = time_now;
        if(thread_max < std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count())
			thread_max = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();
    }
    else
    {
        recv(opt->ADDR, opt->data_in, opt->data_in_size);
    }
    pthread_exit((void*)opt);
}

int ServerDuetORAM::send(std::string ADDR, unsigned char* input, TYPE_INDEX inputSize)
{
    zmq::context_t context(1);
    zmq::socket_t socket(context,ZMQ_REQ);

    socket.connect(ADDR.c_str());
	
    unsigned char buffer_in[sizeof(CMD_SUCCESS)];

    try
    {
		cout<< "	[ThreadedSocket] Sending to " << ADDR << endl;
		socket.send (input, inputSize);
		cout<< "	[ThreadedSocket] Data SENT!" << ADDR << endl;
        
        socket.recv(buffer_in, sizeof(CMD_SUCCESS));
        cout<< "	[ThreadedSocket] ACK RECEIVED!" << ADDR << endl;
    }
    catch (exception &ex)
    {
        goto exit;
    }

exit:
	socket.disconnect(ADDR.c_str());
	socket.close();
	return 0;
}

int ServerDuetORAM::recv(std::string ADDR, unsigned char* output, TYPE_INDEX outputSize)
{
    zmq::context_t context(1);
    zmq::socket_t socket(context,ZMQ_REP);
	
    socket.bind(ADDR.c_str());

    try
    {
		cout<< "	[ThreadedSocket] Waiting Client on " << ADDR << endl;
		socket.recv (output, outputSize);
		cout<< "	[ThreadedSocket] Data RECEIVED! " << ADDR <<endl;
        
        socket.send((unsigned char*)CMD_SUCCESS,sizeof(CMD_SUCCESS));
        cout<< "	[ThreadedSocket] ACK SENT! "  << ADDR<<endl;
    }
    catch (exception &ex)
    {
        cout<<"Socket error!";
        goto exit;
    }
    
exit:
	socket.close();
	return 0;
}
