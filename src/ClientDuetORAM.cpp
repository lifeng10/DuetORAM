#include "ClientDuetORAM.hpp"
#include "DuetORAM.hpp"

unsigned long int ClientDuetORAM::exp_logs[9];
unsigned long int ClientDuetORAM::thread_max = 0; //用于记录执行线程的时间。因为有多个线程，只记录耗时最长的线程所用的时间
char ClientDuetORAM::timestamp[16];

ClientDuetORAM::ClientDuetORAM() {
    this->pos_map = new TYPE_POS_MAP[NUM_BLOCK+1];
    this->metaData = new TYPE_DATA*[NUM_NODES];

    for (int i = 0; i < NUM_NODES; i++)
    {
        this->metaData[i] = new TYPE_ID[BUCKET_SIZE];
    }

    // 生成的查询向量
    this->sharedVector = new uint8_t*[NUM_SERVERS];     // vector for retrieval, XOR {0,1}
	for(int i = 0; i < NUM_SERVERS; i++)
	{
		this->sharedVector[i] = new uint8_t[(H+1)*BUCKET_SIZE];
	}

    // 收两个服务器返回的检索结果
    retrievedShare = new TYPE_DATA*[NUM_SERVERS];       // retrieval result: block shares from servers
	for(int k = 0 ; k < NUM_SERVERS; k++)
    {
        retrievedShare[k] = new TYPE_DATA[DATA_CHUNKS];
    }

    // 恢复查询block结果
    recoveredBlock = new TYPE_DATA[DATA_CHUNKS];

    // 发送查询向量给server
    this->vector_buffer_out = new unsigned char*[NUM_SERVERS];
    for (TYPE_INDEX i = 0; i < NUM_SERVERS; i++)
    {
        this->vector_buffer_out[i] = new unsigned char[sizeof(TYPE_INDEX)+(H+1)*BUCKET_SIZE*sizeof(uint8_t)]; 
    }

    // 接收查询结果，2个服务器，DATA_CHUNKS个数据
    blocks_buffer_in = new unsigned char*[NUM_SERVERS];
    for(int i = 0; i < NUM_SERVERS ; i++)
    {
        blocks_buffer_in[i] = new unsigned char[sizeof(TYPE_DATA)*DATA_CHUNKS];
    }

    // 检索完后写回block，包括读的次数和iv
    this->block_buffer_out = new unsigned char*[NUM_SERVERS];
    for (int i = 0 ; i < NUM_SERVERS; i ++)
    {
        this->block_buffer_out[i]= new unsigned char[sizeof(TYPE_DATA)*DATA_CHUNKS+sizeof(TYPE_INDEX)+sizeof(block)];
        memset(this->block_buffer_out[i], 0, sizeof(TYPE_DATA)*DATA_CHUNKS+sizeof(TYPE_INDEX)+sizeof(block) );
    }
}

ClientDuetORAM::~ClientDuetORAM() {
    
}

int ClientDuetORAM::init() {
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
    

    auto start = time_now;
    auto end = time_now;

    for (TYPE_INDEX i = 1; i <= NUM_BLOCK; i++) {
        this->pos_map[i].pathID = -1;
        this->pos_map[i].pathIdx = -1;
    }

    start = time_now;
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
            // socket.recv(buffer_in,sizeof(CMD_SUCCESS));
            // socket.disconnect(ADDR.c_str());
        }
        else if (i == 1)
        {
            socket.send((unsigned char*)&this->k2, sizeof(block),0);
            // socket.recv(buffer_in,sizeof(CMD_SUCCESS));
            // socket.disconnect(ADDR.c_str());
        }  
        socket.recv(buffer_in,sizeof(CMD_SUCCESS));
        // socket.disconnect(ADDR.c_str());
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

    // 2. create select query
	uint8_t logicVector[(H+1)*BUCKET_SIZE];
	
	auto start = time_now;
    auto end = time_now;

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

        thread_args[i] = struct_socket(SERVER_ADDR[i]+ ":" + std::to_string(SERVER_PORT+i*NUM_SERVERS+i), vector_buffer_out[i], sizeof(pathID)+(H+1)*BUCKET_SIZE*sizeof(uint8_t), blocks_buffer_in[i], sizeof(TYPE_DATA)*DATA_CHUNKS, CMD_REQUEST_BLOCK, NULL);

        pthread_create(&thread_sockets[i], NULL, &ClientDuetORAM::thread_socket_func, (void*)&thread_args[i]);
    }
    
    // 初始化结果
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
    cout<< "	[ClientDuetORAM] All Shares Retrieved in " << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count()<< " ns"<<endl;
    exp_logs[2] = thread_max;
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
	exp_logs[3] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();

    cout << "	[ClientDuetORAM] Block-" << recoveredBlock[0] <<" is Retrieved" <<endl;
    if (recoveredBlock[0] == blockID)
        cout << "	[ClientDuetORAM] SUCCESS!!!!!!" << endl;
    else
        cout << "	[ClientDuetORAM] ERROR!!!!!!!!" << endl;
		
    assert(recoveredBlock[0] == blockID && "ERROR: RECEIEVED BLOCK IS NOT CORRECT!!!!!!");

    // 6. update position map
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
    
    // 8. upload the share to numRead-th slot in root bucket
    for(TYPE_INDEX k = 0; k < NUM_SERVERS; k++) 
    {
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

    cout << "================================================================" << endl;
	cout << "ACCESS OPERATION FOR BLOCK-" << blockID << " COMPLETED." << endl; 
	cout << "================================================================" << endl;


    

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





