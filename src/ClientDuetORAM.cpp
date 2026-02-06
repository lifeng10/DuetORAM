#include "ClientDuetORAM.hpp"
#include "DuetORAM.hpp"
#include <thread>
#include <chrono>

unsigned long int ClientDuetORAM::exp_logs[19];
unsigned long int ClientDuetORAM::thread_max = 0;
char ClientDuetORAM::timestamp[16];

ClientDuetORAM::ClientDuetORAM() {
    this->pos_map = new TYPE_POS_MAP[NUM_BLOCK+1];
    this->metaData = new TYPE_DATA*[NUM_NODES];

    for (int i = 0; i < NUM_NODES; i++)
    {
        this->metaData[i] = new TYPE_ID[BUCKET_SIZE];
    }

    this->sharedVector = new uint8_t*[NUM_SERVERS];     // vector for retrieval, XOR {0,1}
	for(int i = 0; i < NUM_SERVERS; i++)
	{
		this->sharedVector[i] = new uint8_t[(H+1)*BUCKET_SIZE];
	}

    retrievedShare = new TYPE_DATA*[NUM_SERVERS];       // retrieval result: block shares from servers
	for(int k = 0 ; k < NUM_SERVERS; k++)
    {
        retrievedShare[k] = new TYPE_DATA[DATA_CHUNKS];
    }

    recoveredBlock = new TYPE_DATA[DATA_CHUNKS];

    this->vector_buffer_out = new unsigned char*[NUM_SERVERS];
    for (TYPE_INDEX i = 0; i < NUM_SERVERS; i++)
    {
        this->vector_buffer_out[i] = new unsigned char[sizeof(TYPE_INDEX)+(H+1)*BUCKET_SIZE*sizeof(uint8_t)]; 
    }

    blocks_buffer_in = new unsigned char*[NUM_SERVERS];
    for(int i = 0; i < NUM_SERVERS ; i++)
    {
        blocks_buffer_in[i] = new unsigned char[sizeof(TYPE_DATA)*DATA_CHUNKS];
    }

    this->block_buffer_out = new unsigned char*[NUM_SERVERS];
    for (int i = 0 ; i < NUM_SERVERS; i ++)
    {
        this->block_buffer_out[i]= new unsigned char[sizeof(TYPE_DATA)*DATA_CHUNKS+sizeof(TYPE_INDEX)+sizeof(block)];
        memset(this->block_buffer_out[i], 0, sizeof(TYPE_DATA)*DATA_CHUNKS+sizeof(TYPE_INDEX)+sizeof(block) );
    }

    // eviction variables
    this->SIZE_PI = (H+2)*BUCKET_SIZE;
    this->u1 = new TYPE_INDEX[SIZE_PI];
    this->u2 = new TYPE_INDEX[SIZE_PI];

    this->permutationAa = new block[SIZE_PI * SIZE_PI];
    this->permutationBb = new block[SIZE_PI * SIZE_PI];

    this->sub_pi_1 = new TYPE_INDEX[SIZE_PI];
    this->sub_pi_2 = new TYPE_INDEX[SIZE_PI];

    this->circularShift_1 = new TYPE_INDEX[SIZE_PI];
    this->circularShift_2 = new TYPE_INDEX[SIZE_PI];


    this->key_permutation_buffer_out = new block*[NUM_SERVERS];
    for (int i = 0 ; i < NUM_SERVERS; i++)
    {
        this->key_permutation_buffer_out[i] = new block[SIZE_PI * SIZE_PI + 1]; // +1 for key
    }


    this->evict_pi = new TYPE_INDEX[SIZE_PI];


    this->evict_buffer_out = new unsigned char*[NUM_SERVERS];
    for (int i = 0; i < NUM_SERVERS; i++)
    {
        this->evict_buffer_out[i] = new unsigned char[sizeof(TYPE_INDEX) + sizeof(block) + 3 * sizeof(TYPE_INDEX) * SIZE_PI];
    }

    time_t now = time(0);
	char* dt = ctime(&now);
	FILE* file_out = NULL;
	string path = clientLocalDir + "lastest_config";
	string info = "Height of Tree: " + to_string(HEIGHT) + "\n";
	info += "Number of Blocks: " + to_string(NUM_BLOCK) + "\n";
	info += "Bucket Size: " + to_string(BUCKET_SIZE) + "\n";
	info += "Eviction Rate: " + to_string(EVICT_RATE) + "\n";
	info += "Block Size (B): " + to_string(BLOCK_SIZE) + "\n";
	info += "ID Size (B): " + to_string(sizeof(TYPE_ID)) + "\n";
	info += "Number of Chunks: " + to_string(DATA_CHUNKS) + "\n";
	info += "Total Size of Data (MB): " + to_string((NUM_BLOCK*(BLOCK_SIZE+sizeof(TYPE_ID)))/1048576.0) + "\n";
	info += "Total Size of ORAM (MB): " + to_string(BUCKET_SIZE*NUM_NODES*(BLOCK_SIZE+sizeof(TYPE_ID))/1048576.0) + "\n";
	
	#if defined(PRECOMP_MODE)
		info += "PRECOMPUTATION MODE: Active\n";
	#else
		info += "PRECOMPUTATION MODE: Inactive\n";
	#endif 
	
	if((file_out = fopen(path.c_str(),"w+")) == NULL){
		cout<< "	File Cannot be Opened!!" <<endl;
		exit;
	}
	fputs(dt, file_out);
	fputs(info.c_str(), file_out);
	fclose(file_out);
	
	tm *now_time = localtime(&now);
	if(now != -1)
		strftime(timestamp,16,"%d%m_%H%M",now_time);
    
}

ClientDuetORAM::~ClientDuetORAM() {
    
}

int ClientDuetORAM::init() {
    auto start = time_now;
    auto end = time_now;

    start = time_now;

    this->numRead = 0;
    this->numEvict = 0;
    PRG().random_block(&this->k1, 1);
    PRG().random_block(&this->k2, 1);

    for (int i = 0; i < NUM_SERVERS; i++)
    {
        FILE* key_out = NULL;
        string path_key = keysPath + to_string(i+1);
        if((key_out = fopen(path_key.c_str(),"wb+")) == NULL)
        {
            cout<< "	[init] File Cannot be Opened!!" <<endl;
            exit(0);
        }
        if (i == 0)
        {
            fwrite(&this->k1, 1, sizeof(block), key_out);
        }
        else
        {
            fwrite(&this->k2, 1, sizeof(block), key_out);
        }
        fclose(key_out);
    }
      

    for (TYPE_INDEX i = 1; i <= NUM_BLOCK; i++) {
        this->pos_map[i].pathID = -1;
        this->pos_map[i].pathIdx = -1;
    }

    
    DuetORAM ORAM;
    ORAM.build(this->pos_map, this->metaData, this->k1, this->k2);
    end = time_now;

    cout<<endl;
    cout<< "Elapsed Time for Setup on Disk: "<<std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count()<<" ns"<<endl;
    cout<<endl;

    std::ofstream output;
    string path2 = clientLocalDir + "lastest_config";
    output.open(path2, std::ios_base::app);
    output<< "INITIALIZATION ON CLIENT: Performed\n";
    output.close();

    FILE* local_data = NULL;
	if((local_data = fopen(clientTempPath.c_str(),"wb+")) == NULL){
		cout<< "	[init] File Cannot be Opened!!" <<endl;
		exit(0);
	}

    fwrite(this->pos_map, 1, (NUM_BLOCK+1)*sizeof(TYPE_POS_MAP), local_data);
	fwrite(&this->numEvict, sizeof(this->numEvict), 1, local_data);
	fwrite(&this->numRead, sizeof(this->numRead), 1, local_data);
    fwrite(&this->k1, sizeof(this->k1), 1, local_data);
    fwrite(&this->k2, sizeof(this->k2), 1, local_data);
	fclose(local_data);

    return 0;
}

int ClientDuetORAM::sendORAMTree() {
    unsigned char* oram_buffer_out = new unsigned char [BUCKET_SIZE*sizeof(TYPE_DATA)*DATA_CHUNKS]; 
    memset(oram_buffer_out,0, BUCKET_SIZE*sizeof(TYPE_DATA)*DATA_CHUNKS);
    unsigned char* oram_buffer_iv_out = new unsigned char [BUCKET_SIZE*sizeof(block)];
    memset(oram_buffer_iv_out,0, BUCKET_SIZE*sizeof(block));
    unsigned char* key_buffer_out = new unsigned char [sizeof(block)];

    int CMD = CMD_SEND_ORAM_TREE;       
    unsigned char buffer_in[sizeof(CMD_SUCCESS)];
	unsigned char buffer_out[sizeof(CMD)];

    memcpy(buffer_out, &CMD,sizeof(CMD));

    zmq::context_t context(1);
    zmq::socket_t socket(context,ZMQ_REQ);

    auto start = time_now;

    for (int i = 0; i < NUM_SERVERS; i++)
    {
        string ADDR = SERVER_ADDR[i]+ ":" + std::to_string(SERVER_PORT+i*NUM_SERVERS+i); 
        cout<< "	[sendORAMTree] Connecting to " << ADDR <<endl;
        socket.connect( ADDR.c_str());

        socket.send(buffer_out, sizeof(CMD));
		cout<< "	[sendORAMTree] Command SENT! " << CMD <<endl;
        socket.recv(buffer_in, sizeof(CMD_SUCCESS));

        for (TYPE_INDEX j = 0 ; j < NUM_NODES; j++)
        {
            //load data to buffer
            FILE* fdata = NULL;
            string path = rootPath + to_string(i) + "/" + to_string(j);
            if((fdata = fopen(path.c_str(),"rb")) == NULL)
            {
                cout<< "	[sendORAMTree] File Cannot be Opened!!" <<endl;
                exit(0);
            }
            long lSize;
            fseek (fdata , 0 , SEEK_END);
            lSize = ftell (fdata);
            rewind (fdata);
            if(fread(oram_buffer_out ,1 , BUCKET_SIZE*sizeof(TYPE_DATA)*DATA_CHUNKS, fdata) != sizeof(char)*lSize){
                cout<< "	[sendORAMTree] File loading error be Read!!" <<endl;
                exit(0);
            }
            fclose(fdata);
            //send to server i
            socket.send(oram_buffer_out,BUCKET_SIZE*sizeof(TYPE_DATA)*DATA_CHUNKS,0);
            socket.recv(buffer_in,sizeof(CMD_SUCCESS));

            //load iv1 to buffer
            FILE* fdata_iv = NULL;
            string path_iv = rootPath + "client_iv" + to_string(i+1) + "/" + to_string(j);
            if((fdata_iv = fopen(path_iv.c_str(),"rb")) == NULL)
            {
                cout<< "	[sendORAMTree] File Cannot be Opened!!" <<endl;
                exit(0);
            }
            long lSize_iv;
            fseek (fdata_iv , 0 , SEEK_END);
            lSize_iv = ftell (fdata_iv);
            rewind (fdata_iv);
            if(fread(oram_buffer_iv_out ,1 , BUCKET_SIZE*sizeof(block), fdata_iv) != sizeof(char)*lSize_iv){
                cout<< "	[sendORAMTree] File loading error be Read!!" <<endl;
                exit(0);
            }
            fclose(fdata_iv);
            //send to server i
            socket.send(oram_buffer_iv_out,BUCKET_SIZE*sizeof(block),0);
            socket.recv(buffer_in,sizeof(CMD_SUCCESS));
        }
        //send keys
        if (i == 0)
        {
            memcpy(key_buffer_out, &this->k1, sizeof(block));
        }
        else
        {
            memcpy(key_buffer_out, &this->k2, sizeof(block));
        }
        
        socket.send(key_buffer_out, sizeof(block),0);
        socket.recv(buffer_in,sizeof(CMD_SUCCESS));
        socket.disconnect( ADDR.c_str());
    }
    socket.close();

    auto end = time_now;
    cout<< "	[sendORAMTree] ORAM Tree SENT in " << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count() << " ns"<<endl;

    return 0;
}

int ClientDuetORAM::load() {
    FILE* local_data = NULL;
	if((local_data = fopen(clientTempPath.c_str(),"rb")) == NULL){
		cout<< "	[load] File Cannot be Opened!!" <<endl;
		exit(0);
	}

    long lSize;
	fseek (local_data , 0 , SEEK_END);
	lSize = ftell (local_data);
	rewind (local_data);

    unsigned char* local_data_buffer = new unsigned char[sizeof(char)*lSize];
	if(fread(local_data_buffer ,1 , sizeof(char)*lSize, local_data) != sizeof(char)*lSize){
		cout<< "	[load] File Cannot be Read!!" <<endl;
		exit(0);
	}
	fclose(local_data);

    size_t currSize = 0;
	memcpy(this->pos_map, &local_data_buffer[currSize], (NUM_BLOCK+1)*sizeof(TYPE_POS_MAP));
	currSize += (NUM_BLOCK+1)*sizeof(TYPE_POS_MAP);
	memcpy(&this->numEvict, &local_data_buffer[currSize], sizeof(this->numEvict));
	cout << "[load] Eviction number: "<<numEvict << endl;
	currSize += sizeof(this->numEvict);
	memcpy(&this->numRead, &local_data_buffer[currSize], sizeof(this->numRead));
	cout << "[load] Retrieval number: "<<numRead << endl;
    currSize += sizeof(this->numRead);
    memcpy(&this->k1, &local_data_buffer[currSize], sizeof(this->k1));
    cout << "[load] Key k1: " << this->k1 << endl;
    currSize += sizeof(this->k1);
    memcpy(&this->k2, &local_data_buffer[currSize], sizeof(this->k2));
    cout << "[load] Key k2: " << this->k2 << endl;

    //scan position map to load bucket metaData (for speed optimization)
    TYPE_INDEX fullPathIdx[H+1];
    DuetORAM ORAM;
    for(TYPE_INDEX i = 1 ; i < NUM_BLOCK+1; i++)
    {
        ORAM.getFullPathIdx(fullPathIdx,pos_map[i].pathID);
        this->metaData[fullPathIdx[pos_map[i].pathIdx/ BUCKET_SIZE]][pos_map[i].pathIdx%BUCKET_SIZE] = i;
    }

    std::ofstream output;
	string path = clientLocalDir + "lastest_config";
	output.open(path, std::ios_base::app);
	output<< "SETUP FROM LOCAL DATA\n";
	output.close();

    // ===================send key to server===================
    unsigned char* key_buffer_out = new unsigned char[sizeof(block)];
    memset(key_buffer_out, 0, sizeof(block));
    int CMD = CMD_SEND_KEYS;
    unsigned char buffer_in[sizeof(CMD_SUCCESS)];
    unsigned char buffer_out[sizeof(CMD)];
    memcpy(buffer_out, &CMD,sizeof(CMD));

    zmq::context_t context(1);
    for (int i = 0; i < NUM_SERVERS; i++)
    {
        zmq::socket_t socket(context,ZMQ_REQ);
        // socket.set(zmq::sockopt::linger, 0);
        string ADDR = SERVER_ADDR[i]+ ":" + std::to_string(SERVER_PORT+i*NUM_SERVERS+i); 
        cout<< "	[sendORAMKey] Connecting to " << ADDR <<endl;
        socket.connect( ADDR.c_str());

        socket.send(buffer_out, sizeof(CMD));
		cout<< "	[sendORAMKey] Command SENT! " << CMD <<endl;
        socket.recv(buffer_in, sizeof(CMD_SUCCESS));

        if (i == 0)
        {
            socket.send((unsigned char*)&this->k1, sizeof(block),0);
        }
        else if (i == 1)
        {
            socket.send((unsigned char*)&this->k2, sizeof(block),0);
        }  
        socket.recv(buffer_in,sizeof(CMD_SUCCESS));
        socket.close();
    }
    // ========================================================
	
	delete[] local_data_buffer;
    delete[] key_buffer_out;

    return 0;
}

int ClientDuetORAM::access(TYPE_ID blockID)
{
    DuetORAM ORAM;
	cout << "================================================================" << endl;
	cout << "STARTING ACCESS OPERATION FOR BLOCK-" << blockID <<endl; 
	cout << "================================================================" << endl;

    // 1. get the path & index of the block of interest
    TYPE_INDEX pathID = pos_map[blockID].pathID;
    TYPE_INDEX pathIdx = pos_map[blockID].pathIdx;
    block iv1 = pos_map[blockID].iv1;
    block iv2 = pos_map[blockID].iv2;
	cout << "	[ClientTripORAM] PathID = " << pathID <<endl;
    cout << "	[ClientTripORAM] Location = " << pos_map[blockID].pathIdx <<endl;
	uint8_t logicVector[(H+1)*BUCKET_SIZE];
	
	auto start = time_now;
    auto end = time_now;
    auto start_retrieval = time_now;
    auto end_retrieval = time_now;

    start_retrieval = time_now;
    start = time_now;
	getLogicalVector(logicVector, blockID);
	end = time_now;
	cout<< "	[ClientTripORAM] Logical Vector Created in " << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count()<< " ns"<<endl;
    exp_logs[0] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();

    // 3. create query shares
    start = time_now;
	ORAM.getSharedVector(logicVector, this->sharedVector);
	end = time_now;
	cout<< "	[ClientTripORAM] Shared Vector Created in " << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count()<< " ns"<<endl;
	exp_logs[1] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();

    // 4. send to server & receive the answer
    start = time_now;
    struct_socket thread_args[NUM_SERVERS];
    for (int i = 0; i < NUM_SERVERS; i++)
    {
        memcpy(&vector_buffer_out[i][0], &pathID, sizeof(pathID));
        memcpy(&vector_buffer_out[i][sizeof(pathID)], &this->sharedVector[i][0], (H+1)*BUCKET_SIZE*sizeof(uint8_t));
        // COMMUNICATION:
        thread_args[i] = struct_socket(SERVER_ADDR[i]+ ":" + std::to_string(SERVER_PORT+i*NUM_SERVERS+i), vector_buffer_out[i], sizeof(pathID)+(H+1)*BUCKET_SIZE*sizeof(uint8_t), blocks_buffer_in[i], sizeof(TYPE_DATA)*DATA_CHUNKS, CMD_REQUEST_BLOCK, NULL);
        pthread_create(&thread_sockets[i], NULL, &ClientDuetORAM::thread_socket_func, (void*)&thread_args[i]);
    }
    

    memset(recoveredBlock, 0, sizeof(TYPE_DATA)*DATA_CHUNKS);

    for (int i = 0; i < NUM_SERVERS; i++)
    {
        memset(retrievedShare[i],0,sizeof(TYPE_DATA)*DATA_CHUNKS);
    }
    
    for (int i = 0; i < NUM_SERVERS; i++)
    {
        pthread_join(thread_sockets[i], NULL);
        memcpy(retrievedShare[i],blocks_buffer_in[i],sizeof(TYPE_DATA)*DATA_CHUNKS);
        cout << "	[ClientDuetORAM] From Server-" << i+1 << " => BlockID = " << retrievedShare[i][0]<< endl;
    }

    end = time_now;
    end_retrieval = time_now;
    cout<< "	[ClientDuetORAM] All Shares Retrieved in " << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count()<< " ns"<<endl;
    exp_logs[2] = thread_max;
    exp_logs[3] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();
    exp_logs[4] = std::chrono::duration_cast<std::chrono::nanoseconds>(end_retrieval-start_retrieval).count();
    thread_max = 0;


    // 5. recover the block
    start = time_now;

    TYPE_DATA* keystream_1 = new TYPE_DATA[DATA_CHUNKS];
    memset(keystream_1,0,sizeof(TYPE_DATA)*DATA_CHUNKS);
    TYPE_DATA* keystream_2 = new TYPE_DATA[DATA_CHUNKS];
    memset(keystream_2,0,sizeof(TYPE_DATA)*DATA_CHUNKS);

    aes_128_ctr<TYPE_DATA>(k1, iv1, nullptr, (uint8_t*)keystream_1, sizeof(TYPE_DATA)*DATA_CHUNKS);
    aes_128_ctr<TYPE_DATA>(k2, iv2, nullptr, (uint8_t*)keystream_2, sizeof(TYPE_DATA)*DATA_CHUNKS);

    for (int i = 0; i < DATA_CHUNKS; i++)
    {
        recoveredBlock[i] = retrievedShare[0][i] ^ retrievedShare[1][i] ^ keystream_1[i] ^ keystream_2[i];
    }

    end = time_now;
    cout<< "	[ClientDuetORAM] Recovery Done in " << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count()<< " ns"<<endl;
	exp_logs[5] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();

    cout << "	[ClientDuetORAM] Block-" << recoveredBlock[0] <<" is Retrieved" <<endl;
    if (recoveredBlock[0] == blockID)
        cout << "	[ClientDuetORAM] SUCCESS!!!!!!" << endl;
    else
        cout << "	[ClientDuetORAM] ERROR!!!!!!!!" << endl;
		
    assert(recoveredBlock[0] == blockID && "ERROR: RECEIEVED BLOCK IS NOT CORRECT!!!!!!");

    // 6. update position map
    // clear old metadata
    start = time_now;
    TYPE_INDEX fullPathIdx[H+1];
    ORAM.getFullPathIdx(fullPathIdx,pathID);
    this->metaData[fullPathIdx[pos_map[blockID].pathIdx / BUCKET_SIZE ]][pos_map[blockID].pathIdx % BUCKET_SIZE] = 0; // Clear the corresponding block position

    // assign new random position
    pos_map[blockID].pathID = Utils::RandBound(N_leaf)+(N_leaf-1);
    pos_map[blockID].pathIdx = numRead;
    this->metaData[0][numRead] = blockID;
    PRG().random_block(&iv1, 1);
    PRG().random_block(&iv2, 1);
    pos_map[blockID].iv1 = iv1;
    pos_map[blockID].iv2 = iv2;

    // 7. Create new shares for the retrieved block
    memset(keystream_1,0,sizeof(TYPE_DATA)*DATA_CHUNKS);
    memset(keystream_2,0,sizeof(TYPE_DATA)*DATA_CHUNKS);
    aes_128_ctr<TYPE_DATA>(k1, iv1, nullptr, (uint8_t*)keystream_1, sizeof(TYPE_DATA)*DATA_CHUNKS);
    aes_128_ctr<TYPE_DATA>(k2, iv2, nullptr, (uint8_t*)keystream_2, sizeof(TYPE_DATA)*DATA_CHUNKS);
    TYPE_DATA* newBlock = new TYPE_DATA[DATA_CHUNKS];
    for (int i = 0; i < DATA_CHUNKS; i++)
    {
        newBlock[i] = recoveredBlock[i] ^ keystream_1[i] ^ keystream_2[i];
    }

    for (int i = 0; i < NUM_SERVERS; i++)
    {
        memcpy(&block_buffer_out[i][0], newBlock, sizeof(TYPE_DATA)*DATA_CHUNKS);
        memcpy(&block_buffer_out[i][sizeof(TYPE_DATA)*DATA_CHUNKS], &numRead, sizeof(TYPE_INDEX));
        if (i == 0)
        {
            memcpy(&block_buffer_out[i][sizeof(TYPE_DATA)*DATA_CHUNKS+sizeof(TYPE_INDEX)], &iv1, sizeof(block));
        }
        else
        {
            memcpy(&block_buffer_out[i][sizeof(TYPE_DATA)*DATA_CHUNKS+sizeof(TYPE_INDEX)], &iv2, sizeof(block));
        }
    }
    end = time_now;
    exp_logs[6] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();
    
    // 8. upload the share to numRead-th slot in root bucket
    start = time_now;
    for(TYPE_INDEX k = 0; k < NUM_SERVERS; k++) 
    {   
        // COMMUNICATION:
        thread_args[k] = struct_socket(SERVER_ADDR[k]+ ":" + std::to_string(SERVER_PORT+k*NUM_SERVERS+k), block_buffer_out[k], sizeof(TYPE_DATA)*DATA_CHUNKS+sizeof(TYPE_INDEX)+sizeof(block), NULL, 0, CMD_SEND_BLOCK,NULL);

		pthread_create(&thread_sockets[k], NULL, &ClientDuetORAM::thread_socket_func, (void*)&thread_args[k]);
    }

    this->numRead = (this->numRead+1)%EVICT_RATE;
    cout << "	[ClientDuetORAM] Number of Read = " << this->numRead <<endl;

    for (int i = 0; i < NUM_SERVERS; i++)
    {
        pthread_join(thread_sockets[i], NULL);
        cout << "	[ClientDuetORAM] Block upload completed!" << endl;
    }
    end = time_now;
    exp_logs[7] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();

    cout << "================================================================" << endl;
	cout << "ACCESS OPERATION FOR BLOCK-" << blockID << " COMPLETED." << endl; 
	cout << "================================================================" << endl;

    

    // 9. Perform eviction if needed
    auto eviction_start = time_now;
    auto eviction_end = time_now;
    auto start_memcpy = time_now;
    auto end_memcpy = time_now;
    if (this->numRead == 0)
    {
        cout << "================================================================" << endl;
		cout << "STARTING EVICTION-" << this->numEvict+1 <<endl;
		cout << "================================================================" << endl;

        // 9.1 generate permutation matrices        //NOTE: Offline process
        cout << "================================================================" << endl;
        cout << "STARTING INITIALIZE PERMUTATION MATRICES!" <<endl; 
        cout << "================================================================" << endl;

        start = time_now;
        PRG prg_permutation;
        SecretSharedShuffle sss;
        prg_permutation.random_block(&this->key3, 1);
        prg_permutation.random_block(&this->key4, 1);
        sss.initialize(this->key3, this->key4, this->permutationAa, this->permutationBb, this->u1, this->u2);
        end = time_now;
        exp_logs[8] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();
        

        // 9.2 send punctured matrix and secret key to servers
        start = time_now;
        memcpy(&key_permutation_buffer_out[0][0], &this->key4, sizeof(block));  //NOTE: key4 with permutationAa
        memcpy(&key_permutation_buffer_out[0][1], &permutationAa[0], sizeof(block)*(H+2)*BUCKET_SIZE*(H+2)*BUCKET_SIZE);
        memcpy(&key_permutation_buffer_out[1][0], &this->key3, sizeof(block));  //NOTE: key3 with permutationBb
        memcpy(&key_permutation_buffer_out[1][1], &permutationBb[0], sizeof(block)*(H+2)*BUCKET_SIZE*(H+2)*BUCKET_SIZE);
        // COMMUNICATION:
        sendInitialPermutation(this->key_permutation_buffer_out);
        end = time_now;
        exp_logs[9] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();
        cout << "   [sendInitialPermutation] SENDING INITIALIZE PERMUTATION MATRICES FINISHED!" <<endl; 

        std::this_thread::sleep_for(std::chrono::milliseconds(10000));

        // 9.3 generate shuffle
        eviction_start = time_now;
        start = time_now;
        blocks.clear();
        // 9.3.1 read blocks from the path and sibling bucket of leaf bucket
        TYPE_INDEX evict_pathID = ORAM.getEvictLeafID(numEvict);
        TYPE_INDEX pIDH_sibling = getSibling(P(evict_pathID,H));
        readSiblingBucket(pIDH_sibling);
        for (int i = 0; i < H+1; i++)
        {
            TYPE_INDEX bucketID = P(evict_pathID, i);
            readBucket(bucketID);
        }

        // 9.3.2 generate shuffle
        block seed_iv1;
        block seed_iv2;
        sss.generateShuffle(this->blocks, evict_pathID, this->evict_pi, this->pos_map, this->metaData, seed_iv1, seed_iv2);
        createSubPermutation(this->sub_pi_1, this->sub_pi_2);
        
        end = time_now;
        cout<< "	[ClientDuetORAM] Evict Permutation Created in " << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count()<< " ns"<<endl;
		exp_logs[10] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();

        // 9.4 generate circular shift for shared permutation
        start = time_now;
        for (TYPE_INDEX j = 0; j < SIZE_PI; j++)
        {
            this->circularShift_1[j] = (this->sub_pi_1[j] - this->u1[j] + SIZE_PI) % SIZE_PI;
            this->circularShift_2[j] = (this->sub_pi_2[j] - this->u2[j] + SIZE_PI) % SIZE_PI;
        }
        end = time_now;
        exp_logs[11] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();

        // eviction information for server 0
        
        start_memcpy = time_now;
        memcpy(&evict_buffer_out[0][0], &evict_pathID, sizeof(TYPE_INDEX));
        memcpy(&evict_buffer_out[0][sizeof(TYPE_INDEX)], &seed_iv1, sizeof(block));
        memcpy(&evict_buffer_out[0][sizeof(TYPE_INDEX)+sizeof(block)], &sub_pi_1[0], sizeof(TYPE_INDEX)*SIZE_PI);
        memcpy(&evict_buffer_out[0][sizeof(TYPE_INDEX)+sizeof(block)+sizeof(TYPE_INDEX)*SIZE_PI], &circularShift_1[0], sizeof(TYPE_INDEX)*SIZE_PI);
        memcpy(&evict_buffer_out[0][sizeof(TYPE_INDEX)+sizeof(block)+2*sizeof(TYPE_INDEX)*SIZE_PI], &circularShift_2[0], sizeof(TYPE_INDEX)*SIZE_PI);

        // eviction information for server 1
        memcpy(&evict_buffer_out[1][0], &evict_pathID, sizeof(TYPE_INDEX));
        memcpy(&evict_buffer_out[1][sizeof(TYPE_INDEX)], &seed_iv2, sizeof(block));
        memcpy(&evict_buffer_out[1][sizeof(TYPE_INDEX)+sizeof(block)], &sub_pi_2[0], sizeof(TYPE_INDEX)*SIZE_PI);
        memcpy(&evict_buffer_out[1][sizeof(TYPE_INDEX)+sizeof(block)+sizeof(TYPE_INDEX)*SIZE_PI], &circularShift_1[0], sizeof(TYPE_INDEX)*SIZE_PI);
        memcpy(&evict_buffer_out[1][sizeof(TYPE_INDEX)+sizeof(block)+2*sizeof(TYPE_INDEX)*SIZE_PI], &circularShift_2[0], sizeof(TYPE_INDEX)*SIZE_PI);
        end_memcpy = time_now;
        exp_logs[15] = std::chrono::duration_cast<std::chrono::nanoseconds>(end_memcpy-start_memcpy).count();

        // 9.5 send eviction information to servers
        start = time_now;
        for (int i = 0; i < NUM_SERVERS; i++)
        {
            // COMMUNICATION:
            thread_args[i] = struct_socket(SERVER_ADDR[i] + ":" + std::to_string(SERVER_PORT+i*NUM_SERVERS+i), evict_buffer_out[i], sizeof(TYPE_INDEX) + sizeof(block) + 3 * sizeof(TYPE_INDEX) * SIZE_PI, NULL, 0, CMD_SEND_EVICT, NULL);
            pthread_create(&thread_sockets[i], NULL, &ClientDuetORAM::thread_socket_func, (void*)&thread_args[i]);
        }
        
        for (int i = 0; i < NUM_SERVERS; i++)
        {
            pthread_join(thread_sockets[i], NULL);
        }

        end = time_now;
        eviction_end = time_now;
        exp_logs[14] = std::chrono::duration_cast<std::chrono::nanoseconds>(eviction_end-eviction_start).count();
        exp_logs[12] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();
        cout<< "	[ClientDuetORAM] Eviction Permutation Send in " << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count()<< " ns"<<endl;

        exp_logs[13] = thread_max;
        thread_max = 0;
		
		cout << "================================================================" << endl;
		cout << "EVICTION-" << this->numEvict+1 << " COMPLETED" << endl;
		cout << "================================================================" << endl;
        
        this->numEvict = (numEvict+1) % N_leaf;
    }

    // 11. store local info to disk
    FILE* local_data = NULL;
	if((local_data = fopen(clientTempPath.c_str(),"wb+")) == NULL){
		cout<< "	[ClientTripORAM] File Cannot be Opened!!" <<endl;
		exit(0);
	}
	fwrite(this->pos_map, 1, (NUM_BLOCK+1)*sizeof(TYPE_POS_MAP), local_data);
	fwrite(&this->numEvict, sizeof(this->numEvict), 1, local_data);
	fwrite(&this->numRead, sizeof(this->numRead), 1, local_data);
	fwrite(&this->k1, sizeof(this->k1), 1, local_data);
	fwrite(&this->k2, sizeof(this->k2), 1, local_data);
	fclose(local_data);
     
	// 12. write log
	Utils::write_list_to_file(to_string(HEIGHT)+"_" + to_string(BLOCK_SIZE)+"_client_" + timestamp + ".txt",logDir, exp_logs, 19);
	memset(exp_logs, 0, sizeof(unsigned long int)*19);
        

    return 0;
}

int ClientDuetORAM::getLogicalVector(uint8_t* logicVector, TYPE_ID blockID)
{
    TYPE_INDEX loc = pos_map[blockID].pathIdx;
    memset(logicVector, 0, sizeof(uint8_t)*BUCKET_SIZE*(H+1));
    logicVector[loc] = 1;

    return 0;
}

void* ClientDuetORAM::thread_socket_func(void* args)
{
    struct_socket* opt = (struct_socket*) args;
    sendNrecv(opt->ADDR, opt->data_out, opt->data_out_size, opt->data_in, opt->data_in_size, opt->CMD);
    pthread_exit((void*) opt);
}

int ClientDuetORAM::sendNrecv(std::string ADDR, unsigned char* data_out, size_t data_out_size, unsigned char* data_in, size_t data_in_size, int CMD)
{
    zmq::context_t context(1);
    zmq::socket_t socket(context,ZMQ_REQ);
    socket.connect(ADDR.c_str());
	
    unsigned char buffer_in[sizeof(CMD_SUCCESS)];
	unsigned char buffer_out[sizeof(CMD)];

    try
    {
        cout<< "	[ThreadSocket] Sending Command to "<< ADDR << endl;
        memcpy(buffer_out, &CMD,sizeof(CMD));
        socket.send(buffer_out, sizeof(CMD));
		cout<< "	[ThreadSocket] Command SENT! " << CMD <<endl;
        socket.recv(buffer_in, sizeof(CMD_SUCCESS));

        auto start = time_now;
		cout<< "	[ThreadSocket] Sending Data..." << endl;
		socket.send (data_out, data_out_size);
		cout<< "	[ThreadSocket] Data SENT!" << endl;
        if(data_in_size == 0)
            socket.recv(buffer_in,sizeof(CMD_SUCCESS));
        else
            socket.recv(data_in,data_in_size);
            
		auto end = time_now;
		if(thread_max < std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count())
			thread_max = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();
    }
    catch(const std::exception& e)
    {
        cout<< "	[ThreadSocket] Socket error!"<<endl;
		exit(0);
    }
    socket.disconnect(ADDR.c_str());

    return 0;
}

int ClientDuetORAM::readBucket(TYPE_INDEX bucketID)
{
    struct_block block;
    for (int i = 0; i < BUCKET_SIZE; i++)
    {
        block.blockID = this->metaData[bucketID][i];
        if (block.blockID != 0)
        {
            block.pathID = this->pos_map[block.blockID].pathID;
            block.pathIdx = this->pos_map[block.blockID].pathIdx;
            block.iv1 = this->pos_map[block.blockID].iv1;
            block.iv2 = this->pos_map[block.blockID].iv2;
            this->blocks.push_back(block);
        }
    }
    
    return 0;
}

int ClientDuetORAM::readSiblingBucket(TYPE_INDEX bucketID)
{
    struct_block block;
    for (int i = 0; i < BUCKET_SIZE; i++)
    {
        block.blockID = this->metaData[bucketID][i];
        if (block.blockID != 0)
        {
            block.pathID = this->pos_map[block.blockID].pathID;
            block.pathIdx = this->pos_map[block.blockID].pathIdx + BUCKET_SIZE;
            block.iv1 = this->pos_map[block.blockID].iv1;
            block.iv2 = this->pos_map[block.blockID].iv2;
            this->blocks.push_back(block);
        }
    }
    
    return 0;
}

TYPE_INDEX ClientDuetORAM::P(TYPE_INDEX pathID, int l)
{
    return (TYPE_INDEX)((1<<l) - 1 + (((pathID+1) % N_leaf) >> (H - l)));
}

TYPE_INDEX ClientDuetORAM::getSibling(TYPE_INDEX pIDl)
{
    return (pIDl % 2) ? (pIDl + 1) : (pIDl - 1);
}

int ClientDuetORAM::sendInitialPermutation(block** key_permutation_buffer_out)
{
    int CMD = CMD_SEND_INITIALIZATION_PERMUTATION;
    unsigned char buffer_in[sizeof(CMD_SUCCESS)];
    unsigned char buffer_out[sizeof(CMD)];

    memcpy(buffer_out, &CMD,sizeof(CMD));

    zmq::context_t context(1);
    zmq::socket_t socket(context,ZMQ_REQ);

    for (int i = 0; i < NUM_SERVERS; i++)
    {
        string ADDR = SERVER_ADDR[i]+ ":" + std::to_string(SERVER_PORT+i*NUM_SERVERS+i); 
        cout<< "	[sendInitialPermutation] Connecting to " << ADDR <<endl;
        socket.connect( ADDR.c_str());

        socket.send(buffer_out, sizeof(CMD));
		cout<< "	[sendInitialPermutation] Command SENT! " << CMD <<endl;
        socket.recv(buffer_in, sizeof(CMD_SUCCESS));

        socket.send(key_permutation_buffer_out[i], sizeof(block)*((H+2)*BUCKET_SIZE * (H+2)*BUCKET_SIZE + 1), 0);
        socket.recv(buffer_in, sizeof(CMD_SUCCESS));
        socket.disconnect( ADDR.c_str());
    }
    socket.close();

    return 0;
}

int ClientDuetORAM::createSubPermutation(TYPE_INDEX* pi_1, TYPE_INDEX* pi_2)
{
    //create random permutation pi_1
    generateRandomPermutation(pi_1, BUCKET_SIZE*(H+2));

    //create inverse permutation of pi_1
    // vector<TYPE_INDEX> inverse_pi_1 = inversePermutation(pi_1);
    TYPE_INDEX* inverse_pi_1 = new TYPE_INDEX[BUCKET_SIZE*(H+2)];
    inversePermutation(pi_1, inverse_pi_1, BUCKET_SIZE*(H+2));

    //create permutation pi_2
    for (TYPE_INDEX i = 0; i < BUCKET_SIZE*(H+2); i++)
    {
        pi_2[i] = inverse_pi_1[this->evict_pi[i]];
    }
    
    return 0;
}

void ClientDuetORAM::generateRandomPermutation(TYPE_INDEX* permutation, TYPE_INDEX size)
{
    iota(permutation, permutation+size, 0);
    random_shuffle(permutation, permutation+size);
}

void ClientDuetORAM::inversePermutation(TYPE_INDEX* permutation, TYPE_INDEX* inverse, TYPE_INDEX size)
{
    for (TYPE_ID i = 0; i < size; i++)
    {
        inverse[permutation[i]] = i;
    }
}



