/*
 * Template for group 6 traffic light system
 * includes status update and initializing communication with control node
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <errno.h>
#include <pthread.h>
#include <mqueue.h>
#include <sys/stat.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>


#define replyBuf 10
#define IDBuf 10
#define ID "UI"					// define Node ID here for each node (I1 used as an example)


const char * sendMqueueLocation = "/net/Control_Node/Send_queue";  			// one queue on control node for receiving data from this node
const char * rcvMqueueLocation = "/net/Control_Node/UI_rcv_queue";
char rcvBuffer[1000];														// receive message buffer



struct updateMsg {				// message that gets sent to control with current status of system
	char senderID[IDBuf];       // our data (unique id from client)
	int currentState;			// current state of the state machine
	int timeSinceLastChange;	// time since the state machine last changed state
	int carState;				// corresponding car event for each state
	int pedState;				// corresponding pedestrian event for each state
	bool trainstate;
};
struct updateMsg uM;

struct controlMsg {				// message received form control
	char senderID[IDBuf];       // control data (unique id)
	bool failState;				// force fail state
};
struct controlMsg cM;

struct ForwardMsg {				// message received form control
	char senderID[IDBuf];       // control data (unique id)
	char RecevierID[IDBuf];		// where we are sending the message
	int currentState;			// current state of state machine
	bool trainApproach;			// train approaching or not
};
struct ForwardMsg fM;

struct replyMsg {				// the reply message for when a message is received from control
	char senderID[IDBuf];
    char buf[replyBuf];			// Message we send back to clients to tell them the messages was processed correctly.
};
struct replyMsg rM;

// queues
mqd_t mqs, mqr;

// mutexes
pthread_mutex_t statusUpdateMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t rcvQueueMutex = PTHREAD_MUTEX_INITIALIZER;
struct updateMsg i1Status, i2Status, x1Status;
bool gotI1 = false, gotI2 = false, gotX1 = false;
bool failReceived = false;
bool failValue = false;
pthread_cond_t statusChanged = PTHREAD_COND_INITIALIZER;
bool newStatus = false;


// State names - confirm these match the I1, I2 and X1 enums.
const char *getI1StateName(int state){
    switch (state){
        case 0: return "ALL RED";
        case 1: return "R3 GREEN";
        case 2: return "R3 YELLOW";
        case 3: return "R1 GREEN";
        case 4: return "R1 YELLOW";
        case 5: return "WAITING FOR TRAIN";
        case 6: return "FAIL";
        default: return "UNKNOWN";
    }
}

const char *getI2StateName(int state){
    switch (state){
        case 0: return "ALL RED";
        case 1: return "R3 GREEN";
        case 2: return "R3 YELLOW";
        case 3: return "R2 GREEN";
        case 4: return "R2 YELLOW";
        case 5: return "WAITING FOR TRAIN";
        case 6: return "FAIL";
        default: return "UNKNOWN";
    }
}

const char *getX1StateName(int state){
    switch (state){
        case 0: return "NORMAL";
        case 1: return "TRAIN CROSSING";
        case 2: return "FAILSAFE";
        default: return "UNKNOWN";
    }
}


int initialiseComChannel(void){
	// Connect to the queues created by the control node.
	// just really an initial check to see if queue exists before starting the system up
	// check connection to send queue
	printf("Connecting to %s queue on control node, please wait\n" ,sendMqueueLocation);
	int mqueueEstablished = 0;														// for while loop
	strlcpy(rM.buf, "SEND_EOK", sizeof(rM.buf));									// confirmation message for send queue
	while(!mqueueEstablished){														// loop until mqueue is opened successfully (need a timeout???)
		mqs = mq_open(sendMqueueLocation, O_WRONLY);								// opens queue on control node if it exists
		if (mqs != -1){																// if connection is good
			if (mq_send(mqs,(const char*)&rM, sizeof(rM), 0) == -1){
				perror("mq_send");
				return EXIT_FAILURE;
			}
			printf("-->connection to %s on control node confirmed!\n", sendMqueueLocation);
			mqueueEstablished = 1;													// exit while loop
		} else {																	// else no connection
			printf(">");															// just to show system is not frozen.
			fflush(stdout);
		}
		usleep(500000);																	// update to hw timer maybe (probably doesnt matter at this stage in the code)
	}

	// check connection to rcv queue
	printf("\nConnecting to %s queue on control node, please wait\n", rcvMqueueLocation);
	mqueueEstablished = 0;															// reset while loop variable
	strlcpy(rM.buf, "RCV_EOK", sizeof(rM.buf));										// confirmation message for send queue
	while(!mqueueEstablished){														// loop until mqueue is opened successfully (need a timeout???)
		mqr = mq_open(rcvMqueueLocation, O_RDONLY);									// opens queue on control node if it exists
		if (mqr != -1){																// if connection is good
			if (mq_send(mqs,(const char*)&rM, sizeof(rM), 0) == -1){				// send confirmation to control (should we pull data from queue to confirm working?)
				perror("mq_send");
				return EXIT_FAILURE;
			}
			printf("-->connection to %s on control node confirmed!\n", rcvMqueueLocation);
			mqueueEstablished = 1;													// exit while loop
		} else {																	// else no connection
		}
		//sleep(1);																	// update to hw timer maybe (probably doesnt matter at this stage in the code)
	}
	// note that the queues remain open so you don't need to use mq_open just mq_send/receive when sending or reading messages.
	return EXIT_SUCCESS;
}


void *statusUpdate(void *data){
	while(1){
		sleep(1);
		pthread_mutex_lock(&statusUpdateMutex);				// lock for status update struct use (maybe change to add time limit later)
		if (mq_send(mqs, (char *)&uM, sizeof(uM), 0) == -1)
            perror("mq_send status");
		pthread_mutex_unlock(&statusUpdateMutex);			// unlock to allow writing to status update struct by other threads
	}
	return NULL;
}

void *inputProcessing(void *data){
    char command[20];
    printf("UI terminal: enter r to request fail reset (0)\n");
    while (fgets(command, sizeof(command), stdin) != NULL) {
        if (command[0] == 'r') {
            struct controlMsg resetMsg = {0};
            strlcpy(resetMsg.senderID, ID, sizeof(resetMsg.senderID));
            resetMsg.failState = false;
            if (mq_send(mqs, (char *)&resetMsg, sizeof(resetMsg), 0) == -1)
                perror("mq_send reset request");
            else
                printf("Reset request sent (Control Node does not currently process it).\n");
        }
    }
    return NULL;
}


void *outputProcessing(void *data){
    while (1) {
        pthread_mutex_lock(&rcvQueueMutex);
        while (!newStatus)
            pthread_cond_wait(&statusChanged, &rcvQueueMutex);
        newStatus = false;

        printf("\n--- SYSTEM STATUS ---\n");
        if (gotI1) printf("I1 state=%s time=%d car=%d ped=%d train=%d\n",
            getI1StateName(i1Status.currentState), i1Status.timeSinceLastChange,
            i1Status.carState, i1Status.pedState, i1Status.trainstate);
        else printf("I1: waiting for update\n");
        if (gotI2) printf("I2 state=%s time=%d car=%d ped=%d train=%d\n",
            getI2StateName(i2Status.currentState), i2Status.timeSinceLastChange,
            i2Status.carState, i2Status.pedState, i2Status.trainstate);
        else printf("I2: waiting for update\n");
        if (gotX1) printf("X1 state=%s time=%d car=%d ped=%d train=%d\n",
            getX1StateName(x1Status.currentState), x1Status.timeSinceLastChange,
            x1Status.carState, x1Status.pedState, x1Status.trainstate);
        else printf("X1: waiting for update\n");
        if (failReceived) printf("Fail state: %d\n", failValue ? 1 : 0);
        else printf("Fail state: waiting for control update\n");
        fflush(stdout);
        pthread_mutex_unlock(&rcvQueueMutex);
    }
    return NULL;
}




void *rcvQueue(void *data){
    while (1) {
        ssize_t n = mq_receive(mqr, rcvBuffer, sizeof(rcvBuffer), NULL);
        if (n == -1) { perror("mq_receive"); sleep(1); continue; }
        pthread_mutex_lock(&rcvQueueMutex);
        if ((size_t)n == sizeof(uM)) {
            struct updateMsg received;
            memcpy(&received, rcvBuffer, sizeof(received));
            if (!strcmp(received.senderID, "I1")) { i1Status = received; gotI1 = true; }
            else if (!strcmp(received.senderID, "I2")) { i2Status = received; gotI2 = true; }
            else if (!strcmp(received.senderID, "X1")) { x1Status = received; gotX1 = true; }
        } else if ((size_t)n == sizeof(cM)) {
            memcpy(&cM, rcvBuffer, sizeof(cM));
            failValue = cM.failState;
            failReceived = true;
        } else if ((size_t)n == sizeof(fM)) {
            memcpy(&fM, rcvBuffer, sizeof(fM));
        } else if ((size_t)n == sizeof(rM)) {
            memcpy(&rM, rcvBuffer, sizeof(rM));
        } else {
            printf("Unexpected message size: %ld\n", (long)n);
            pthread_mutex_unlock(&rcvQueueMutex);
            continue;
        }
        // Tell the display thread that the status has changed.
        newStatus = true;
        pthread_cond_signal(&statusChanged);
        pthread_mutex_unlock(&rcvQueueMutex);
    }
    return NULL;
}


int main(void) {
	// define this node for sending messages
		// update message
	memset(&uM, 0, sizeof(uM)); 						// Clears the array
	strlcpy(uM.senderID, ID, sizeof(uM.senderID));		// copies string into struct element
		// reply message
	memset(&rM, 0, sizeof(rM)); 						// Clears the array
	strlcpy(rM.senderID, ID, sizeof(rM.senderID));		// copies string into struct element
		// forward message
	memset(&fM, 0, sizeof(fM)); 						// Clears the array
	strlcpy(fM.senderID, ID, sizeof(fM.senderID));		// copies string into struct element

	// check communication channel
	if (initialiseComChannel() != EXIT_SUCCESS) return EXIT_FAILURE;

	// initialise threads (default for now maybe change to round robin later if time)
	pthread_t mr, su, ip, op;
	pthread_create(&mr,NULL,rcvQueue, NULL);
	pthread_create(&su,NULL,statusUpdate, NULL);
	pthread_create(&ip,NULL,inputProcessing, NULL);
	pthread_create(&op,NULL,outputProcessing, NULL);
	
	pthread_join(mr, NULL);
	return EXIT_SUCCESS;
}
