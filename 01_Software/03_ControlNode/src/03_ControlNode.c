/*
 * Control Node Process
 * Created by: Jaime Zizman
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <errno.h>
#include <pthread.h>
#include <mqueue.h>
#include <sys/stat.h>

// ------------------------------------------ Definitions ------------------------------------------ //

#define replyBuf 10
#define IDBuf 10
#define receiverBuf 10
#define ID "C1"					// define Node ID here for each node
#define Q_FLAGS O_RDWR | O_CREAT | O_EXCL
#define Q_Mode S_IRUSR | S_IWUSR
#define msgSz 1000

// ------------------------------------------ Global Variables ------------------------------------------ //

const char * sendMqueueLocation = "/Send_queue";  			// one queue on control node for receiving data from this node
const char * I1rcvMqueueLocation = "/I1_rcv_queue"; 		// unique queue on control node for sending data to this node
const char * I2rcvMqueueLocation = "/I2_rcv_queue"; 		// unique queue on control node for sending data to this node
const char * X1rcvMqueueLocation = "/X1_rcv_queue"; 		// unique queue on control node for sending data to this node
const char * UIrcvMqueueLocation = "/UI_rcv_queue"; 		// unique queue on control node for sending data to this node
char rcvBuffer[1000];										// receive message buffer

enum msgRcv { 	nomsg,
				msguM,
				msgcM,
				msgfM,
				msgrM,
};
enum msgRcv msgRcvStruct;

// ------------------------------------------ Structs ------------------------------------------ //

struct updateMsg {				// message that gets sent to control with current status of system
	char senderID[IDBuf];       // our data (unique id from client)
	int currentState;			// current state of the state machine
	bool sensorState;			// if vehicle/train is present or not (1/0)
	int timeSinceLastChange;	// time since the state machine last changed state
	int phaseState;				// ignore for now - implement if time later
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
mqd_t mqs, mqrI1, mqrI2, mqrX1, mqrUI;

// mutexes
pthread_mutex_t statusUpdateMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t sendQueueMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t msgRcvCond = PTHREAD_COND_INITIALIZER;

// ------------------------------------------ functions ------------------------------------------ //
void *sendQueue(void *data);

// ######################################################################## //
// ############## Establish communications with control Node ############## //
// ######################################################################## //
int initialiseComChannel(void){
	// set up queue attributes
	struct  mq_attr  attr;
	attr.mq_maxmsg = 100;		// max message queue amount
	attr.mq_msgsize = msgSz;	// max message size (make dynamic maybe???)
	attr.mq_flags = 0;
	attr.mq_curmsgs = 0;
	attr.mq_sendwait = 0;
	attr.mq_recvwait = 0;
	struct mq_attr * my_attr = &attr;

	printf("Opening queues for communication between nodes.\n");

	mqs	  = mq_open(sendMqueueLocation, Q_FLAGS, Q_Mode, my_attr);		// open send queue
	mqrI1 = mq_open(I1rcvMqueueLocation, Q_FLAGS, Q_Mode, my_attr);		// open rcv queue for Node I1
	mqrI2 = mq_open(I2rcvMqueueLocation, Q_FLAGS, Q_Mode, my_attr);		// open rcv queue for Node I2
	mqrX1 = mq_open(X1rcvMqueueLocation, Q_FLAGS, Q_Mode, my_attr);		// open rcv queue for Node X1
	mqrUI = mq_open(UIrcvMqueueLocation, Q_FLAGS, Q_Mode, my_attr);		// open rcv queue for Node UI

	if (mqs == -1 || mqrI1 == -1 || mqrI2 == -1 || mqrX1 == -1 || mqrUI == -1){										// check for failure opening queue
		perror("mq_open");
		return EXIT_FAILURE;
	} else {
		printf("All queues successfully opened, verifying connection with nodes.\n");
	}
	// create sendqueue thread now that queues are created
	pthread_t sq;
	pthread_create(&sq,NULL,sendQueue, NULL);


	//loop until all rcv good from nodes
	bool rcvgood = false;		// variable to block until we receive all the msg we are expecting
	int rcvI1 = 0;				// rcv count
	int rcvI2 = 0;				// rcv count
	int rcvX1 = 0;				// rcv count
	int rcvUI = 0;				// rcv count
	while (!rcvgood){																		// while we havent received all messages
		if (msgRcvStruct == msgrM){																// if an rM struct is received
			pthread_mutex_lock(&sendQueueMutex);												// lock mutex critical section
			msgRcvStruct = nomsg;																	// reset enum to nomsg
			if ((!strcmp(rM.buf, "SEND_EOK")) || (!strcmp(rM.buf, "RCV_EOK"))) {			// check for expected msg
				printf("-->received confirmation: %s from node %s\n", rM.buf, rM.senderID);
			} else {																		// else unexpected msg (check worker node code)
				printf("-->unexpected response %s from node %s\n", rM.buf, rM.senderID);
			}
			if (!strcmp(rM.senderID, "I1")){												//check for sender ID
				rcvI1++;																	// if I1 then increment rcv count
			} else if (!strcmp(rM.senderID, "I2")) {										//check for sender ID
				rcvI2++;																	// if I2 then increment rcv count
			} else if (!strcmp(rM.senderID, "X1")) {										//check for sender ID
				rcvX1++;																	// if X1 then increment rcv count
			} else if (!strcmp(rM.senderID, "UI")) {										//check for sender ID
				rcvUI++;																	// if UI then increment rcv count
			} else {																		// else unexpected ID, check worker node code
				printf("Error --> Unexpected sender ID\n");
			}
			pthread_mutex_unlock(&sendQueueMutex);											// unlock mutex (sendQueue thread can now take mutex and update struct for next msg)
		}
		if ((rcvI1 >= 2) && (rcvI2 >= 2) && (rcvX1 >= 2) && (rcvUI >= 2)){					// Check for if we have received all expected responses
			rcvgood = 1;																	// if we have update blocking variable and exit while loop
			printf("Verified connection with all nodes!\n");
		}
	}
	// might want to add timeout for above, will also need to add a retry on the worker nodes
	// above code could have a better implementation, need to check for send and rcv independently in case of resend from worker node


	return EXIT_SUCCESS;
}

// ------------------------------------------ Threads ------------------------------------------ //

// ################################################## //
// ############## Status Update Thread ############## //
// ################################################## //
void *statusUpdate(void *data){							// might update to have higher priority later
	bool alive = true;									// allow us to terminate if need be (currently unused)
	while(alive){
		sleep(1);										// sleep for 1 second, i will replace with HW timer later
		pthread_mutex_lock(&statusUpdateMutex);				// lock for status update struct use (maybe change to add time limit later)
		mq_send(mqs, (char *)&uM, sizeof(uM), 0);		// send status update to control node
		pthread_mutex_unlock(&statusUpdateMutex);			// unlock to allow writing to status update struct by other threads
	}
	return EXIT_SUCCESS;
}

// ################################################## //
// ############## Input Thread ###################### //
// ################################################## //
void *inputProcessing(void *data){

	return EXIT_SUCCESS;
}


// ################################################## //
// ############## Output Thread ##################### //
// ################################################## //
void *outputProcessing(void *data){

	return EXIT_SUCCESS;
}


// ################################################## //
// ############## State Machine ##################### //
// ################################################## //
void *stateMachine(void *data){
	switch (msgRcvStruct){
	case nomsg:
		//nop
		break;
	case msguM:
		//nop
		break;
	case msgcM:
		//nop
		break;
	case msgfM:
		//nop
		break;
	case msgrM:
		//nop
		break;
	}

	return EXIT_SUCCESS;
}

// ################################################## //
// ############ Monitor send queue ################## //
// ################################################## //
void *sendQueue(void *data){
	int alive = 1;
	msgRcvStruct = nomsg;
	while(alive){
		ssize_t n = mq_receive(mqs, rcvBuffer, sizeof(rcvBuffer), NULL);  		//wait for the message
		pthread_mutex_lock(&sendQueueMutex);
		if (n == -1) {															// error with msg rcv
			perror("mq_receive");												// print error
		} else if ((size_t)n == sizeof(uM)){									// compare rcv msg with uM length
			memcpy(&uM, rcvBuffer, sizeof(uM));									// save rcv values in uM struct
			msgRcvStruct = msguM;												// update enum
		} else if ((size_t)n == sizeof(cM)){									// compare rcv msg with cM length
			memcpy(&cM, rcvBuffer, sizeof(cM));									// save rcv values in cM struct
			msgRcvStruct = msgcM;												// update enum
		} else if ((size_t)n == sizeof(fM)){									// compare rcv msg with fM length
			memcpy(&fM, rcvBuffer, sizeof(fM));									// save rcv values in fM struct
			msgRcvStruct = msgfM;												// update enum
		} else if ((size_t)n == sizeof(rM)){									// compare rcv msg with rM length
			memcpy(&rM, rcvBuffer, sizeof(rM));									// save rcv values in rM struct
			msgRcvStruct = msgrM;												// update enum
		} else {
			printf("Unexpected message size");
		}
		pthread_mutex_unlock(&sendQueueMutex);
	}
	return EXIT_SUCCESS;
}

// ------------------------------------------ Main Thread ------------------------------------------ //

// ################################## //
// ############## MAIN ############## //
// ################################## //
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
		// forward message
	memset(&cM, 0, sizeof(cM)); 						// Clears the array
	strlcpy(cM.senderID, ID, sizeof(cM.senderID));		// copies string into struct element

	// check communication channel
	initialiseComChannel();

	// initialise threads (default for now maybe change to round robin later if time)
	pthread_t su, ip, op, sm;
	pthread_create(&su,NULL,statusUpdate, NULL);
	pthread_create(&ip,NULL,inputProcessing, NULL);
	pthread_create(&op,NULL,outputProcessing, NULL);
	pthread_create(&sm,NULL,stateMachine, NULL);

	while(1){

	}

	// shutdown
	mq_unlink(sendMqueueLocation);		// close send queue
	mq_unlink(I1rcvMqueueLocation);		// close rcv queue for Node I1
	mq_unlink(I2rcvMqueueLocation);		// close rcv queue for Node I2
	mq_unlink(X1rcvMqueueLocation);		// close rcv queue for Node X1
	mq_unlink(UIrcvMqueueLocation);		// close rcv queue for Node UI
}
