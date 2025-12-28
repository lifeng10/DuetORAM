#ifndef DUET_ORAM_HPP_
#define DUET_ORAM_HPP_

#include "config.h"

class DuetORAM
{
public:
    DuetORAM();
    ~DuetORAM();

    int build(TYPE_POS_MAP* pos_map, TYPE_DATA** metaData, block k1, block k2);
    int getSharedVector(uint8_t* logicalVector, uint8_t** sharedVector);
    int getFullPathIdx(TYPE_INDEX* fullPath, TYPE_INDEX pathID);
    string getEvictString(TYPE_ID n_evict);
    int getEvictIdx (TYPE_INDEX *srcIdx, TYPE_INDEX *destIdx, TYPE_INDEX *siblIdx, string str_evict);
};


#endif // DUET_ORAM_HPP_