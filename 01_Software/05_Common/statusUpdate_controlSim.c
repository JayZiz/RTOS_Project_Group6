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

// ------------------------------------------ Definitions ------------------------------------------ //

#define replyBuf 10
#define IDBuf 10
#define receiverBuf 10
#define ID "C1"					// define Node ID here for each node (I1 used as an example)
#define Q_FLAGS O_RDWR | O_CREAT | O_EXCL
#define Q_Mode S_IRUSR | S_IWUSR
#define msgSz 1000

// ------------------------------------------ Global Variables ------------------------------------------ //

const char * sendMqueueLocation = "/Send_queue";  					// one queue on control node for receiving data from this node
const char * rcvMqueueLocation = "/xxx_rcv_queue"; 				 	// unique queue on control node for sending data to this node (xxx = node host name "need to discuss this")
char rcvBuffer[1000];												// receive message buffer


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

mqd_t mqs, mqr;


// ------------------------------------------ functions ------------------------------------------ //

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

	mqs = mq_open(sendMqueueLocation, Q_FLAGS, Q_Mode, my_attr);		// open send queue
	mqr = mq_open(rcvMqueueLocation, Q_FLAGS, Q_Mode, my_attr);			// open rcv queue

	if (mqs == -1 || mqr == -1){										// check for failure opening queue
		perror("mq_open");
		return EXIT_FAILURE;
	}

	printf("checking for confirmation of send queue\n");

	ssize_t n = mq_receive(mqs, rcvBuffer, sizeof(rcvBuffer), NULL);  		//wait for the message
	if (n == -1) {															// error with msg rcv
		perror("mq_receive");												// print error
		return EXIT_FAILURE;
	} else if ((size_t)n != sizeof(rM)){									// wrong struct received
		printf("-->unexpected message size\n");								// print error
	} else {																// else msg received correctly
		memcpy(&rM, rcvBuffer, sizeof(rM));									// save rcv values in rM struct
	}

	if (!strcmp(rM.buf, "SEND_EOK")) {										// checks if message is SEND_EOK
		printf("-->received confirmation: %s from node %s\n", rM.buf, rM.senderID);
	} else {
		printf("-->unexpected response %s from node %s\n", rM.buf, rM.senderID);
	}

	printf("\nchecking for confirmation of rcv queue\n");
	n = mq_receive(mqs, rcvBuffer, sizeof(rcvBuffer), NULL);  				//wait for the messages
	if (n == -1) {															// error with msg rcv
		perror("mq_receive");												// print error
		return EXIT_FAILURE;
	} else if ((size_t)n != sizeof(rM)){									// wrong struct received
		printf("-->unexpected message size\n");								// print error
	} else {																// else msg received correctly
		memcpy(&rM, rcvBuffer, sizeof(rM));									// save rcv values in rM struct
	}

	if (!strcmp(rM.buf, "RCV_EOK")) {										// checks if message is RCV_EOK
		printf("-->received confirmation: %s from node %s\n", rM.buf, rM.senderID);
	} else {
		printf("-->unexpected response %s from node %s\n", rM.buf, rM.senderID);
	}


	return EXIT_SUCCESS;
}

// ------------------------------------------ Threads ------------------------------------------ //

// ################################################## //
// ############## Status Update Thread ############## //
// ################################################## //
void *statusUpdate(void *data){
	bool alive = true;																						// allow us to terminate if need be (currently unused)
	while(alive){
		sleep(1);																							// sleep for 1 second (should replace with HW timer)
		mq_send(mqs, (const char*)&uM, sizeof(uM), 0);

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

	return EXIT_SUCCESS;
}

// ------------------------------------------ Main (Thread) ------------------------------------------ //

// ################################## //
// ############## MAIN ############## //
// ################################## //
int main(void) {
	// define this node for sending messages
		// update message
	memset(&uM, 0, sizeof(uM)); 						// Clears the array
	snprintf(uM.senderID, sizeof(uM.senderID), ID);		// copies string into struct element
		// reply message
	memset(&rM, 0, sizeof(rM)); 						// Clears the array
	snprintf(rM.senderID, sizeof(rM.senderID), ID);		// copies string into struct element
		// forward message
	memset(&fM, 0, sizeof(fM)); 						// Clears the array
	snprintf(fM.senderID, sizeof(fM.senderID), ID);		// copies string into struct element

	memset(&cM, 0, sizeof(cM)); 						// Clears the array
	snprintf(cM.senderID, sizeof(cM.senderID), ID);		// copies string into struct element

	// check communication channel
	initialiseComChannel();

	// initialise threads (default for now maybe change to round robin)
	pthread_t su, ip, op, sm;
	pthread_create(&su,NULL,statusUpdate, NULL);
	pthread_create(&ip,NULL,inputProcessing, NULL);
	pthread_create(&op,NULL,outputProcessing, NULL);
	pthread_create(&sm,NULL,stateMachine, NULL);

	mq_unlink(sendMqueueLocation);
	mq_unlink(rcvMqueueLocation);


}
