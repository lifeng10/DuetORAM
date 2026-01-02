#ifndef STRUCT_BLOCK_H
#define STRUCT_BLOCK_H

#include "config.h"

typedef struct struct_block
{
    TYPE_ID blockID;
    TYPE_INDEX pathID;
    TYPE_INDEX pathIdx;
    block iv1;
    block iv2;

    struct_block(TYPE_ID blockID, TYPE_INDEX pathID, TYPE_INDEX pathIdx, block iv1, block iv2)
    {
        this->blockID = blockID;
        this->pathID = pathID;
        this->pathIdx = pathIdx;
        this->iv1 = iv1;
        this->iv2 = iv2;
    }

    struct_block()
    {
    }

    ~struct_block()
    {
    }

}struct_block;

#endif