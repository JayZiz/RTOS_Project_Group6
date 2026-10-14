#ifndef COMMON_MESSAGES_H
#define COMMON_MESSAGES_H

#include <stdbool.h>

#define replyBuf 10
#define IDBuf 10
#define receiverBuf 10

struct updateMsg {
    char senderID[IDBuf];
    int currentState;
    union{
    bool sensorState;
    bool trainstate;
    };
    int timeSinceLastChange;
    int phaseState;
};

struct controlMsg {
    char senderID[IDBuf];
    bool failState;
};

struct ForwardMsg {
    char senderID[IDBuf];
    char RecevierID[IDBuf];
    int currentState;
    bool trainApproach;
};

struct replyMsg {
    char senderID[IDBuf];
    char buf[replyBuf];
};

#endif
