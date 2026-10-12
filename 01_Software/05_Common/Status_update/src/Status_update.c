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
#define ID "I1"					// define Node ID here for each node (I1 used as an example)

// ------------------------------------------ Global Variables ------------------------------------------ //

const char * sendMqueueLocation = "/net/Control_Node/Send_queue";  			// one queue on control node for receiving data from this node
const char * rcvMqueueLocation = "/net/Control_Node/xxx_rcv_queue"; 		// unique queue on control node for sending data to this node (xxx = node host name "need to discuss this")
char rcvBuffer[1000];														// receive message buffer


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
mqd_t mqs, mqr;

// mutexes
pthread_mutex_t statusUpdateMutex = PTHREAD_MUTEX_INITIALIZER;

// ------------------------------------------ functions ------------------------------------------ //

// ######################################################################## //
// ############## Establish communications with control Node ############## //
// ######################################################################## //
int initialiseComChannel(void){
	// using mqueue as the minimum, upgrade to native message passing if time\
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
			}						// send confirmation to control (do we need to check control get this msg lmao)
			printf("-->connection to %s on control node confirmed!\n", sendMqueueLocation);
			mqueueEstablished = 1;													// exit while loop
		} else {																	// else no connection
			//printf(">");															// just to show system is not frozen.
		}
		//sleep(1);																	// update to hw timer maybe (probably doesnt matter at this stage in the code)
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
			//printf(">");															// just to show system is not frozen. implement another time, keeps glitching out
		}
		//sleep(1);																	// update to hw timer maybe (probably doesnt matter at this stage in the code)
	}
	// note that the queues remain open so you don't need to use mq_open just mq_send/receive when sending or reading messages.
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

	// check communication channel
	initialiseComChannel();

	// initialise threads (default for now maybe change to round robin later if time)
	pthread_t su, ip, op, sm;
	pthread_create(&su,NULL,statusUpdate, NULL);
	pthread_create(&ip,NULL,inputProcessing, NULL);
	pthread_create(&op,NULL,outputProcessing, NULL);
	pthread_create(&sm,NULL,stateMachine, NULL);
	// continue to add your shit here
}
