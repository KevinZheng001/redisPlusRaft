#include "../../src/redismodule.h"
#include <stdlib.h>
#include <string.h>
void HelloRedis_LogArgs(RedisModuleString **argv, int argc)
{
    for (int j = 0; j < argc; j++) {
        const char *s = RedisModule_StringPtrLen(argv[j],NULL);
        printf("ARGV[%d] = %s\n", j, s);
    }
}

int HelloRedis_RandCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
   
    HelloRedis_LogArgs(argv,argc);
    RedisModule_ReplyWithLongLong(ctx,rand());
    return REDISMODULE_OK;
}

int sunkey() {
    printf("this is sun key\n");
    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
   
    if (RedisModule_Init(ctx,"hello",1,REDISMODULE_APIVER_1) == REDISMODULE_ERR) 
        return REDISMODULE_ERR;
    HelloRedis_LogArgs(argv,argc);
   
    if (RedisModule_CreateCommand(ctx,"hello.rand", HelloRedis_RandCommand,"readonly",0,0,0) 
        == REDISMODULE_ERR)
        return REDISMODULE_ERR;
   
    return REDISMODULE_OK;
}

