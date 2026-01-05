#include "DuetORAM.hpp"

DuetORAM::DuetORAM() {
    
}

DuetORAM::~DuetORAM() {
    
}

int DuetORAM::build(TYPE_POS_MAP* pos_map, TYPE_DATA** metaData, block k1, block k2) {
    int div = ceil(NUM_BLOCK/(double)N_leaf);
    assert(div <= BUCKET_SIZE && "ERROR: CHECK THE PARAMETERS => LEAVES CANNOT STORE ALL");
    PRG prg;

    // bucket: DATA_CHUNKS * BUCKET_SIZE
    TYPE_DATA** bucket = new TYPE_DATA*[DATA_CHUNKS];
    for (int i = 0 ; i < DATA_CHUNKS; i++)
    {
        bucket[i] = new TYPE_DATA[BUCKET_SIZE];
        memset(bucket[i],0,sizeof(TYPE_DATA)*BUCKET_SIZE);
    }
    // bucket_iv: BUCKET_SIZE
    block* bucket_iv1 = new block[BUCKET_SIZE];
    block* bucket_iv2 = new block[BUCKET_SIZE];

    FILE* file_out = NULL;
    string path;
    FILE* file_iv1_out = NULL;
    string path_iv1;
    FILE* file_iv2_out = NULL;
    string path_iv2;
	
    // NOTE: 注释调输出信息以加快初始化速度
    // ======================================================NOTE:======================================================
    // cout << "=================================================================" << endl;
    // cout<< "[DuetORAM] Creating Buckets on Disk" << endl;
    // ======================================================NOTE:======================================================

    boost::progress_display show_progress2(NUM_NODES);

    //generate bucket ID pools
    vector<TYPE_ID> blockIDs;
    for(TYPE_ID i = 0; i <NUM_BLOCK;i++)
    {
        blockIDs.push_back(i+1);
    }

    //random permutation using built-in function
    std::random_shuffle ( blockIDs.begin(), blockIDs.end() );

    // 初始化前一半个空节点
    for (TYPE_INDEX i = 0; i < NUM_NODES/2; i++)
    {
        memset(bucket_iv1,0,sizeof(block)*BUCKET_SIZE);
        memset(bucket_iv2,0,sizeof(block)*BUCKET_SIZE);
        // 把原始节点存放在本地
        file_out = NULL;
        path = clientDataDir + to_string(i);
        if((file_out = fopen(path.c_str(),"wb+")) == NULL)
        {
            cout<< "[DuetORAM] File Cannot be Opened!!" <<endl;
            exit(0);
        }     
        for(int ii = 0 ; ii <DATA_CHUNKS; ii++)
        {
            fwrite(bucket[ii], 1, BUCKET_SIZE*sizeof(TYPE_DATA), file_out);
        }
        fclose(file_out);

        // 把对应bucket中的每个block的iv1存放在本地
        file_iv1_out = NULL;                                     // for storing iv1
        path_iv1 = clientDataDir_iv1 + to_string(i);              // for storing iv1
        if ((file_iv1_out = fopen(path_iv1.c_str(), "wb+")) == NULL)
        {
            cout<< "[DuetORAM] File Cannot be Opened!!" <<endl;
            exit(0);
        }
        prg.random_block(bucket_iv1, BUCKET_SIZE);
        fwrite(bucket_iv1, 1, BUCKET_SIZE*sizeof(block), file_iv1_out);
        fclose(file_iv1_out);


        // 把对应bucket中的每个block的iv2存放在本地
        file_iv2_out = NULL;                                     // for storing iv2
        path_iv2 = clientDataDir_iv2 + to_string(i);              // for storing iv2
        if ((file_iv2_out = fopen(path_iv2.c_str(), "wb+")) == NULL)
        {
            cout<< "[DuetORAM] File Cannot be Opened!!" <<endl;
            exit(0);
        }
        prg.random_block(bucket_iv2, BUCKET_SIZE);
        fwrite(bucket_iv2, 1, BUCKET_SIZE*sizeof(block), file_iv2_out);
        fclose(file_iv2_out);

        ++show_progress2;
    }


    //generate random blocks in leaf-buckets
    //叶子节点第一行要初始化，是因为每次数据都写在第一行了
    //metaData是记录了block存放在哪个bucket的哪个位置
    TYPE_INDEX iter = 0;
    for (TYPE_INDEX i = NUM_NODES/2; i < NUM_NODES; i++)
    {
        memset(bucket[0],0,sizeof(TYPE_DATA)*BUCKET_SIZE);
        memset(bucket_iv1,0,sizeof(block)*BUCKET_SIZE);
        memset(bucket_iv2,0,sizeof(block)*BUCKET_SIZE);

        prg.random_block(bucket_iv1, BUCKET_SIZE);
        prg.random_block(bucket_iv2, BUCKET_SIZE);

        for(int ii = BUCKET_SIZE/2 ; ii<BUCKET_SIZE; ii++)
        {
            if(iter>=NUM_BLOCK)
                break;
            bucket[0][ii] = blockIDs[iter];
            pos_map[blockIDs[iter]].pathID = i;
            pos_map[blockIDs[iter]].pathIdx = ii+(BUCKET_SIZE*H);
            metaData[i][ii]= blockIDs[iter];
            pos_map[blockIDs[iter]].iv1 = bucket_iv1[ii];
            pos_map[blockIDs[iter]].iv2 = bucket_iv2[ii];
            
            iter++;
        }

        //write bucket to file
        file_out = NULL;
        path = clientDataDir + to_string(i);
        if((file_out = fopen(path.c_str(),"wb+")) == NULL)
        {
            cout<< "[DuetORAM] File Cannot be Opened!!" <<endl;
            exit(0);
        }
        for(int ii = 0 ; ii < DATA_CHUNKS; ii++)
        {
            fwrite(bucket[ii], 1, BUCKET_SIZE*sizeof(TYPE_DATA), file_out);
        }
        fclose(file_out);

        //write bucket iv1 to file
        file_iv1_out = NULL;
        path_iv1 = clientDataDir_iv1 + to_string(i);
        if ((file_iv1_out = fopen(path_iv1.c_str(), "wb+")) == NULL)
        {
            cout<< "[DuetORAM] File Cannot be Opened!!" <<endl;
            exit(0);
        }
        fwrite(bucket_iv1, 1, BUCKET_SIZE*sizeof(block), file_iv1_out);
        fclose(file_iv1_out);

        //write bucket iv2 to file
        file_iv2_out = NULL;
        path_iv2 = clientDataDir_iv2 + to_string(i);
        if ((file_iv2_out = fopen(path_iv2.c_str(), "wb+")) == NULL)
        {
            cout<< "[DuetORAM] File Cannot be Opened!!" <<endl;
            exit(0);
        }
        fwrite(bucket_iv2, 1, BUCKET_SIZE*sizeof(block), file_iv2_out);
        fclose(file_iv2_out);

        ++show_progress2;
    }

    cout << "=================================================================" << endl;
    cout<< "[DuetORAM] Creating Shares on Disk" << endl;
    boost::progress_display show_progress(NUM_NODES);


    //初始化每个服务器的bucket
    TYPE_DATA*** bucketShares = new TYPE_DATA**[NUM_SERVERS];
    for(TYPE_INDEX k = 0; k < NUM_SERVERS; k++)
    {
        bucketShares[k] = new TYPE_DATA*[DATA_CHUNKS];
        for(int i = 0 ; i < DATA_CHUNKS ; i++ )
        {
            bucketShares[k][i] = new TYPE_DATA[BUCKET_SIZE];
        }
    }

    TYPE_DATA shares[DATA_CHUNKS][NUM_SERVERS]; //一个值的shares, now the value is identical for two servers
    FILE* file_in = NULL;
    file_out = NULL;
    FILE* file_iv1_in = NULL;
    file_iv1_out = NULL;
    FILE* file_iv2_in = NULL;
    file_iv2_out = NULL;

    // bucket_keystream: BUCKET_SIZE * DATA_CHUNKS
    TYPE_DATA** bucket_keystream_1 = new TYPE_DATA*[BUCKET_SIZE];
    for (int i = 0 ; i < BUCKET_SIZE; i++)
    {
        bucket_keystream_1[i] = new TYPE_DATA[DATA_CHUNKS];
        memset(bucket_keystream_1[i],0,sizeof(TYPE_DATA)*DATA_CHUNKS);
    }

    TYPE_DATA** bucket_keystream_2 = new TYPE_DATA*[BUCKET_SIZE];
    for (int i = 0 ; i < BUCKET_SIZE; i++)
    {
        bucket_keystream_2[i] = new TYPE_DATA[DATA_CHUNKS];
        memset(bucket_keystream_2[i],0,sizeof(TYPE_DATA)*DATA_CHUNKS);
    }

    auto start = time_now;
    auto end = time_now;

    // 计算shares并存储在各个服务器对应的目录下
    for (TYPE_INDEX i = 0; i < NUM_NODES; i++)
    {
        // read iv, and compute keystream
        path_iv1 = clientDataDir_iv1 + to_string(i);
        if ((file_iv1_in = fopen(path_iv1.c_str(), "rb")) == NULL)
        {
            cout<< "[TripORAM] File Cannot be Opened!!" <<endl;
            exit(0);
        }
        fread(bucket_iv1, 1, BUCKET_SIZE*sizeof(block), file_iv1_in);
        fclose(file_iv1_in);

        for (int j = 0; j < BUCKET_SIZE; j++)
        {
            aes_128_ctr<TYPE_DATA>(k1, bucket_iv1[j], nullptr, (uint8_t*)bucket_keystream_1[j], sizeof(TYPE_DATA)*DATA_CHUNKS);
        }


        path_iv2 = clientDataDir_iv2 + to_string(i);
        if ((file_iv2_in = fopen(path_iv2.c_str(), "rb")) == NULL)
        {
            cout<< "[TripORAM] File Cannot be Opened!!" <<endl;
            exit(0);
        }
        fread(bucket_iv2, 1, BUCKET_SIZE*sizeof(block), file_iv2_in);
        fclose(file_iv2_in);

        for (int j = 0; j < BUCKET_SIZE; j++)
        {
            aes_128_ctr<TYPE_DATA>(k2, bucket_iv2[j], nullptr, (uint8_t*)bucket_keystream_2[j], sizeof(TYPE_DATA)*DATA_CHUNKS);
        }

        // read bucket from file
        path = clientDataDir + to_string(i);
        if((file_in = fopen(path.c_str(),"rb")) == NULL){
            cout<< "[TripORAM] File Cannot be Opened!!" <<endl;
            exit(0);
        }
        for(int ii = 0 ; ii < DATA_CHUNKS; ii++)
        {
            fread(bucket[ii] ,1 , BUCKET_SIZE*sizeof(TYPE_DATA), file_in);
            for(TYPE_INDEX jj = 0; jj < BUCKET_SIZE; jj++)
            {
                shares[ii][0] = bucket[ii][jj] ^ bucket_keystream_1[jj][ii] ^ bucket_keystream_2[jj][ii];
                shares[ii][1] = shares[ii][0];
                for(TYPE_INDEX k = 0; k < NUM_SERVERS; k++)  
                {
                    memcpy(&bucketShares[k][ii][jj], &shares[ii][k], sizeof(TYPE_DATA));
                }
            }
        }

        fclose(file_in);

        start = time_now;

        for(TYPE_INDEX k = 0; k < NUM_SERVERS; k++)
        {
            path = rootPath + to_string(k) + "/" + to_string(i);
            if((file_out = fopen(path.c_str(),"wb+")) == NULL)
            {
                cout<< "[TripORAM] File Cannot be Opened!!" <<endl;
                exit;
            }
            for(int ii = 0 ; ii< DATA_CHUNKS ; ii++)
            {
                fwrite(bucketShares[k][ii], 1, BUCKET_SIZE*sizeof(TYPE_DATA), file_out);
            }
            fclose(file_out);
        }

        end = time_now;
        ++show_progress;

    }

    cout<< "[DuetORAM] Elapsed Time for Init on Disk: "<< std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count() <<" ns"<<endl;
    for(TYPE_INDEX k = 0; k < NUM_SERVERS; k++)
    {
        for(int i = 0 ; i < DATA_CHUNKS; i++)
        {
            delete[] bucketShares[k][i];
        }
        delete[] bucketShares[k];
    }
    delete[] bucketShares;
    for(int i = 0 ; i < DATA_CHUNKS; i++)
    {
        delete[] bucket[i];
    }
    delete[] bucket;

    delete[] bucket_iv1;
    delete[] bucket_iv2;
    for (int i = 0 ; i < BUCKET_SIZE; i++)
    {
        delete[] bucket_keystream_1[i];
        delete[] bucket_keystream_2[i];
    }
    delete[] bucket_keystream_1;
    delete[] bucket_keystream_2;
    
    cout<<endl;
	cout << "=================================================================" << endl;
	cout<< "[DuetORAM] Shared ORAM Tree is Initialized for " << NUM_SERVERS << " Servers" <<endl;
	cout << "=================================================================" << endl;
	cout<<endl;


    return 0;
}

int DuetORAM::getFullPathIdx(TYPE_INDEX* fullPath, TYPE_INDEX pathID)
{
    TYPE_INDEX idx = pathID;
    for (int i = H; i >= 0; i--)
    {
		fullPath[i] = idx;
		idx = (idx-1) >> 1;
    }
	
	return 0;
}

int DuetORAM::getSharedVector(uint8_t* logicalVector, uint8_t** sharedVector)
{
    cout << "	[DuetORAM] Starting to Create Retrieve Shares" << endl;

    std::random_device rd;
    std::default_random_engine eng(rd());
    std::uniform_int_distribution<uint8_t> distr(0, 1);

    for (TYPE_INDEX i = 0; i < BUCKET_SIZE*(H+1); i++)
    {
        sharedVector[0][i] = distr(eng);
        sharedVector[1][i] = logicalVector[i] ^ sharedVector[0][i];
    }
    
    return 0;
}

TYPE_INDEX DuetORAM::getEvictLeafID(TYPE_INDEX n_evict)
{
    TYPE_INDEX reversed_val = 0;
    // 1. 手动进行比特反转 (Bit Reversal)
    // 比如 H=3, n_evict=001(1) -> 变成 100(4)
    TYPE_ID temp = n_evict;
    for (int i = 0; i < H; ++i) {
        reversed_val <<= 1;      // 结果左移腾出位置
        reversed_val |= (temp & 1); // 取 temp 的最低位填入
        temp >>= 1;              // temp 右移处理下一位
    }

    // 2. 计算 Root=0 下的叶子节点全局 ID
    // 偏移量：2^H - 1
    return ((1 << H) - 1) + reversed_val;
}





