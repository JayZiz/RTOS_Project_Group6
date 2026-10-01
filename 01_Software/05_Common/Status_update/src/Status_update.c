/*
 * Template for group 6 traffic light system
 * includes status update and initializing communication with control node
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <errno.h>
#include <sys/iofunc.h>
#include <sys/netmgr.h>
#include <pthread.h>

// ------------------------------------------ Definitions ------------------------------------------ //

#define replyBuf 10
#define controlBuf 10
#define senderBuf 10
#define receiverBuf 10

// ------------------------------------------ Global Variables ------------------------------------------ //

int servercoid = 0;

// ------------------------------------------ Structs ------------------------------------------ //

struct updateMsg {		// message that gets sent to control with current status of system
	struct _pulse hdr; 			// Our real data comes after this header
	char SenderID;       		// our data (unique id from client)
	int currentState;			// current state of the state machine
	bool sensorState;			// if vehicle/train is present or not (1/0)
	int timeSinceLastChange;	// time since the state machine last changed state
	int phaseState;				// ignore for now - implement if time later
};
struct updateMsg uM;

struct controlMsg {				// message received form control
	struct _pulse hdr; 			// Our real data comes after this header
	int SenderPID;       		// control data (unique id)
	bool failState;				// force fail state
};
struct controlMsg cM;

struct ForwardMsg {				// message received form control
	struct _pulse hdr; 			// Our real data comes after this header
	char SenderID[senderBuf];       // control data (unique id)
	char RecevierID[receiverBuf];	// where we are sending the message
	int currentState;			// current state of state machine
	bool trainApproach;			// train approaching or not
};
struct controlMsg cM;

struct replyMsg {			// the reply message for when a message is received from control
	struct _pulse hdr;  	// Our real data comes after this header
    char buf[replyBuf];		// Message we send back to clients to tell them the messages was processed correctly.
};
struct replyMsg rM;

struct connectionData {		// struct to contain data required to establish coms with control
	int PID;				// PID of the Node
	int CID;				// CID of the Node
};
struct connectionData cD;		// local node info
struct connectionData cCD;		// control node info


// ------------------------------------------ functions ------------------------------------------ //
size_t read_data(FILE *fp, struct connectionData *p) {
	return (fread(p,sizeof(struct connectionData),1,fp));
}

// ######################################################################## //
// ############## Establish communications with control Node ############## //
// ######################################################################## //
int initialiseComChannel(void){
	controlLocalCom();
	localControlCom();
}

int controlLocalCom(void){
	// control node -> local node communication
	FILE *fp;				// file pointer

	// create channel
	cD.CID = ChannelCreate(_NTO_CHF_DISCONNECT); // _NTO_CHF_DISCONNECT flag used to allow detach
	if (cD.CID == -1)
	{
		printf("\nFailed to create communication channel on server\n");
		return EXIT_FAILURE;
	}

	// write CID and PID to local file
	fp = fopen("tmp/myNodeInfo.info","w");					// create file at location specified (does this need to be unique for each node?)
	if (fp != NULL){										// check for if file was created properly
		cD.PID = getpid();									// get the PID of the local proccess
		printf("PID = %d,   CID = %d\n",cD.PID, cD.CID);	// print to terminal
		fwrite(&cD, sizeof(struct connectionData), 1, fp);	// write struct data to file
		printf("Successfully wrote node information\n");
		fclose(fp);											// force process to close file
	} else {
		printf("Error with opening file\n");
		return EXIT_FAILURE;
	}


	// set up reply (check)
	rM.hdr.type = 0x01;
	rM.hdr.subtype = 0x00;

	// wait for communication confirmation from server
	int rcvid = MsgReceive(cD.CID, &cCD, sizeof(struct connectionData), NULL); 	//waits here for msg (do we need a timeout?)
	if (rcvid == -1) { 														// Error condition, exit
	   printf("\nFailed to MsgReceive\n");
	   return EXIT_FAILURE;
	} else if (rcvid == 0) {												// check for pulse cmd? need to check how this works
		printf("response unsupported\n");									// add functions for this maybe?
		return EXIT_FAILURE;
	} else if (rcvid > 0){													// if msg received then com established
		printf("Control Node confirmed connection success/n");				// PID and CID get written to controlConnectionData (shouldn't need to refresh)
		fflush(stdout);														// idk if this is needed
		printf("    -----> replying with: '%s'\n",rM.buf);					// might remove this
		MsgReply(rcvid, EOK, &rM, sizeof(rM));								// reply to control to confirm msg received. (update so we send control node what this node is incharge of?)
	}

	return EXIT_SUCCESS;
}

int localControlCom(void){
	// local node -> control node communication
	FILE *fp;

	fp = fopen("tmp/myNodeInfo.info", "r");
	if(fp != NULL) {
		while (read_data(fp, &cCD) != 0){
			printf("PID=%d", cCD.PID);
			printf("  CID=%d", cCD.CID);
			printf("\n");
		}
		fclose(fp);
		// Connect to channel
		servercoid = ConnectAttach(ND_LOCAL_NODE, cCD.PID, cCD.CID, _NTO_SIDE_CHANNEL, 0);
		if (servercoid == -1)
		{
			printf("\n    ERROR, could not connect to server!\n\n");
			return EXIT_FAILURE;
		}

		printf("Connection established to process with PID:%d, Ch:%d\n", cCD.PID, cCD.CID);

		//reply

		printf("Sending confirmation that Client has established connnection successfully to server\n");	// empty packet

		if (MsgSend(servercoid, &cM, sizeof(cM), &rM, sizeof(rM)) == -1) {
			printf(" Error data NOT sent to server\n");
				// maybe we did not get a reply
			return EXIT_FAILURE;
		}
		else { // now process the reply
			printf("Server Acknowledged connection\n");
			printf("\n");
		}
		return EXIT_SUCCESS;
	}
	return EXIT_FAILURE;
}

// ------------------------------------------ Threads ------------------------------------------ //

// ################################################## //
// ############## Status Update Thread ############## //
// ################################################## //
void *statusUpdate(void *data){
	bool alive = true;																						// allow us to terminate if need be (currently unused)
	uM.SenderID = cCD.PID;	// need to adjust with enum bullshit																				// pack local PID only need to do once on startup hopefully)
	while(alive){
		sleep(1);																							// sleep for 1 second (maybe replace with HW timer)
		if (MsgSend(servercoid, &uM, sizeof(struct updateMsg), &rM, sizeof(struct replyMsg)) == -1) {			// the struct will be updated by other threads, so critical code here
			printf(" Error in transmitting message");  														// maybe we did not get a reply from the server
		}
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
	// create com channel
	initialiseComChannel();

	// initialise threads (default for now maybe change to round robin)
	pthread_t su, ip, op, sm;
	pthread_create(&su,NULL,statusUpdate, NULL);
	pthread_create(&ip,NULL,inputProcessing, NULL);
	pthread_create(&op,NULL,outputProcessing, NULL);
	pthread_create(&sm,NULL,stateMachine, NULL);



	while(1){

	}
}
