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
#define ID "I2"					// define Node ID here for each node (I1 used as an example)

// ------------------------------------------ Global Variables ------------------------------------------ //

const char * sendMqueueLocation = "/net/Control_Node/Send_queue";  			// one queue on control node for receiving data from this node
const char * rcvMqueueLocation = "/net/Control_Node/I2_rcv_queue"; 		// unique queue on control node for sending data to this node (xxx = node host name "need to discuss this")
char rcvBuffer[1000];														// receive message buffer

enum states { 	State1,
				State2,
				State3,
				State4,
				State5,
				State6,
				State7,
				State8, };
enum states curState;
enum states nxtState;

enum msgRcv { 	nomsg,
				msguM,
				msgcM,
				msgfM,
				msgrM,
};
enum msgRcv msgRcvStruct;

bool alive = true;
bool westVehSens = false;
bool westRtTurnSens = false;
bool southVehSens = false;
bool eastVehSens = false;
// ------------------------------------------ Structs ------------------------------------------ //

struct updateMsg {				// message that gets sent to control with current status of system
	char senderID[IDBuf];       // our data (unique id from client)
	int currentState;			// current state of the state machine
	int timeSinceLastChange;	// time since the state machine last changed state
	bool carState;				// corresponding car event for each state
	bool pedState;				// corresponding pedestrian event for each state
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
pthread_mutex_t SENSOR_MUTEX = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t statusUpdateMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t rcvQueueMutex = PTHREAD_MUTEX_INITIALIZER;


// ------------------------------------------ functions ------------------------------------------ //

// ######################################################################## //
// ############## Establish communications with control Node ############## //
// ######################################################################## //
int initialiseComChannel(void){
	// using mqueue as the minimum, upgrade to native message passing if time
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
				//return EXIT_FAILURE;
			}						// send confirmation to control (do we need to check control get this msg lmao)
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
				//return EXIT_FAILURE;
			}
			printf("-->connection to %s on control node confirmed!\n", rcvMqueueLocation);
			mqueueEstablished = 1;													// exit while loop
		} else {																	// else no connection
			printf(">");															// just to show system is not frozen. implement another time, keeps glitching out
			fflush(stdout);
		}
		usleep(500000);																	// update to hw timer maybe (probably doesnt matter at this stage in the code)
	}
	// note that the queues remain open so you don't need to use mq_open just mq_send/receive when sending or reading messages.
	return EXIT_SUCCESS;
}

// ------------------------------------------ Threads ------------------------------------------ //

// ################################################## //
// ############## Status Update Thread ############## //
// ################################################## //
void *statusUpdate(void *data){																					// allow us to terminate if need be (currently unused)
	while(alive){
		sleep(1);																							// sleep for 1 second (should replace with HW timer)
		pthread_mutex_lock(&statusUpdateMutex);
		if (mq_send(mqs, (const char*)&uM, sizeof(uM), 0) == -1){
			cM.failState = true;																			// control node update this when connection re-established
		}
		pthread_mutex_unlock(&statusUpdateMutex);
	}
	return EXIT_SUCCESS;
}

void *sensorProcessing(void *data){																					// allow us to terminate if need be (currently unused)
	int input = 0;
	while(alive){
		// for terminal input
		printf("Input Sensor for Sim:\n "
				"0 = west vehicle sensor\n"
				"1 = west right turn sensor\n"
				"2 = south vehicle sensor\n"
				"3 = east vehicle sensor\n"
				"4 = Train approaching\n"
				"5 = No sensor\n");
		scanf("%d",&input);
		pthread_mutex_lock(&SENSOR_MUTEX);
		if (input == 0){
			westVehSens = true;
			westRtTurnSens = false;
			southVehSens = false;
			eastVehSens = false;
			fM.trainApproach = false;
		} else if (input == 1){
			westVehSens = false;
			westRtTurnSens = true;
			southVehSens = false;
			eastVehSens = false;
			fM.trainApproach = false;
		} else if (input == 2){
			westVehSens = false;
			westRtTurnSens = false;
			southVehSens = true;
			eastVehSens = false;
			fM.trainApproach = false;
		} else if (input == 3){
			westVehSens = false;
			westRtTurnSens = false;
			southVehSens = false;
			eastVehSens = true;
			fM.trainApproach = false;
		} else if (input == 4){
			westVehSens = false;
			westRtTurnSens = false;
			southVehSens = false;
			eastVehSens = false;
			fM.trainApproach = true;
		} else if (input == 5){
			westVehSens = false;
			westRtTurnSens = false;
			southVehSens = false;
			eastVehSens = false;
			fM.trainApproach = false;
		} else {
			printf("RETARD\n");
			fflush(stdout);
		}
		pthread_mutex_unlock(&SENSOR_MUTEX);
	}
	return EXIT_SUCCESS;
}

// ################################################## //
// ################ msg receive ##################### //
// ################################################## //

void *rcvQueue(void *data){
	int alive = 1;
	msgRcvStruct = nomsg;
	while(alive){
		ssize_t n = mq_receive(mqr, rcvBuffer, sizeof(rcvBuffer), NULL);  		//wait for the message
		pthread_mutex_lock(&rcvQueueMutex);
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
		pthread_mutex_unlock(&rcvQueueMutex);
	}
	return EXIT_SUCCESS;
}

// ################################################## //
// ############## State Machine ##################### //
// ################################################## //
void *stateMachine(void *data){
	curState = State1;				// initial state
	bool printAllowed = true;		// to stop it spaming the terminal
	while(alive){
		pthread_mutex_lock(&SENSOR_MUTEX);
		if(curState != nxtState){		// delays state transitions by 5 seconds for smoother operation
			sleep(5);
			curState = nxtState;
			printAllowed = true;
		}
		switch (curState){
		case State1:
			if(printAllowed){
				printf("-->West and east green.\n");
				printf("-->all else red.\n");
				printAllowed = false;
			}
			if (westRtTurnSens) {
				nxtState = State7;
			} else if (southVehSens){
				nxtState = State2;
			} else if (fM.trainApproach){
				nxtState = State2;
			}
			break;
		case State2:
			if(printAllowed){
				printf("-->West and east yellow.\n");
				printf("-->all else red.\n");
				printAllowed = false;
			}
			nxtState = State3;					// deal with delay in state transition processing
			break;
		case State3:
			if(printAllowed){
				printf("-->South green.\n");
				printf("-->all else red.\n");
				printAllowed = false;
			}
			if (fM.trainApproach == false){		// dont transistion if train approach true
				if (westRtTurnSens || eastVehSens || westVehSens) {
					nxtState = State4;
				}
			}
			break;
		case State4:
			if(printAllowed){
				printf("-->South Yellow.\n");
				printf("-->all else red.\n");
				printAllowed = false;
			}
			nxtState = State5;					// deal with delay in state transition processing
			break;
		case State5:
			if(printAllowed){
				printf("-->West and west right green.\n");
				printf("-->all else red.\n");
				printAllowed = false;
			}
			if (eastVehSens || southVehSens || fM.trainApproach) {
				nxtState = State6;
			}
			break;
		case State6:
			if(printAllowed){
				printf("-->West and west right yellow.\n");
				printf("-->all else red.\n");
				printAllowed = false;
			}
			if (fM.trainApproach) {
				nxtState = State3;
			} else {
				nxtState = State1;
			}
			break;
		case State7:
			if(printAllowed){
				printf("-->West green and east yellow.\n");
				printf("-->all else red.\n");
				printAllowed = false;
			}
			nxtState = State5;
			break;
		case State8:
			if(printAllowed){
				printf("-->Fault detected, flashing red!!!");
				printAllowed = false;
			}
			break;
		}
		pthread_mutex_unlock(&SENSOR_MUTEX);
	}
	return EXIT_SUCCESS;
}


void *inputProcessing(void *data){
	while(alive){
			switch (msgRcvStruct){		// main purpose atm to reset msg to no msg (might use later for more functionality)
			case nomsg:
				// do nothing
				break;
			case msguM:
				// do nothing
				msgRcvStruct = nomsg;
				break;
			case msgcM:
				// do nothing
				msgRcvStruct = nomsg;
				break;
			case msgfM:
				// do nothing
				msgRcvStruct = nomsg;
				break;
			case msgrM:
				// deal with as we go
				msgRcvStruct = nomsg;
				break;
			}
		}
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
	pthread_t su, sm, sp, rq;
	pthread_create(&su,NULL,statusUpdate, NULL);
	pthread_create(&sm,NULL,stateMachine, NULL);
	pthread_create(&sp,NULL,sensorProcessing, NULL);
	pthread_create(&rq,NULL,rcvQueue, NULL);

	while(1){

	}


}
