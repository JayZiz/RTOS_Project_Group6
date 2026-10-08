#include <stdio.h>
#include <stdlib.h>
#include <mqueue.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <string.h>
#include <pthread.h>

#define QUEUE_NAME "/train_node_queue"
typedef enum
{
	S0_NORMAL,
	S1_TRAIN_CROSSING,
	S2_FAILSAFE
}TrainState;// the three required states

typedef enum
{
	EVENT_NONE,
	EVENT_TRAIN_DETECTED,
	EVENT_CROSSING_CLEAR,
	EVENT_FAULT,
	EVENT_FAULT_CLEARED,
	EVENT_QUIT
}TrainEvent;//possible train events

typedef struct
{
    int command;
    int value;
} TrainTestMessage;//POSIX message queue - temporarymessage


TrainEvent pendingEvent = EVENT_NONE;
pthread_mutex_t eventMutex = PTHREAD_MUTEX_INITIALIZER;



//Simulating console output for different states

void applyStateOutputs(TrainState state){
	switch(state){
	case S0_NORMAL:
        printf("\n=== S0: NORMAL ===\n");
        printf("Train: CLEAR\n");
        printf("Crossing: NORMAL\n");
        printf("Warning lights: OFF\n");
        printf("Traffic signal: NORMAL\n");
		break;
	case S1_TRAIN_CROSSING:
        printf("\n=== S1: TRAIN CROSSING ===\n");
        printf("Train: DETECTED\n");
        printf("Crossing: ACTIVE\n");
        printf("Warning lights: ON\n");
        printf("Traffic signal: RED\n");
		break;
	case S2_FAILSAFE:
        printf("\n=== S2: FAIL SAFE ===\n");
        printf("FAULT CONDITION\n");
        printf("Train system: FAILSAFE\n");
        printf("Warning lights: FAILSAFE\n");
        printf("Traffic signal: FAILSAFE\n");
		break;
	}
}
//State machine transition
void processEvent(TrainState *state, TrainEvent event){
	switch(*state){
	case S0_NORMAL:
		if(event==EVENT_TRAIN_DETECTED)
		{
			*state = S1_TRAIN_CROSSING;
		}
		else if(event==EVENT_FAULT)
		{
			*state = S2_FAILSAFE;
		}
		break;
	case S1_TRAIN_CROSSING:
		if(event==EVENT_CROSSING_CLEAR)
		{
			*state = S0_NORMAL;
		}
		else if(event==EVENT_FAULT)
		{
			*state = S2_FAILSAFE;
		}
		break;
	case S2_FAILSAFE:
		if(event==EVENT_FAULT_CLEARED)
		{
			*state = S0_NORMAL;
		}
	}
}

void *inputThread(void *arg)
{
    char input;

    while (1)
    {
        printf("\nCommands:\n");
        printf("  A = Train approaching\n");
        printf("  C = Crossing clear\n");
        printf("  F = Fault\n");
        printf("  R = Fault cleared / reset\n");
        printf("  Q = Quit\n");
        printf("> ");

        scanf(" %c", &input);

        pthread_mutex_lock(&eventMutex);

        switch (input)
        {
            case 'A':
            case 'a':
                pendingEvent = EVENT_TRAIN_DETECTED;
                break;

            case 'C':
            case 'c':
                pendingEvent = EVENT_CROSSING_CLEAR;
                break;

            case 'F':
            case 'f':
                pendingEvent = EVENT_FAULT;
                break;

            case 'R':
            case 'r':
                pendingEvent = EVENT_FAULT_CLEARED;
                break;

            case 'Q':
            case 'q':
                pendingEvent = EVENT_QUIT;
                break;

            default:
                printf("Unknown command.\n");
                break;
        }

        pthread_mutex_unlock(&eventMutex);

        if (input == 'Q' || input == 'q')
        {
            break;
        }
    }

    return NULL;
}
//mqueue testing
void *mqReceiver(void *arg)
{
    mqd_t *mq = (mqd_t *)arg;

    TrainTestMessage msg;

    while (1)
    {
        ssize_t received = mq_receive(
            *mq,
            (char *)&msg,
            sizeof(msg),
            NULL
        );

        if (received == -1)
        {
            perror("mq_receive");
            continue;
        }

        if ((size_t)received != sizeof(msg))
        {
            printf("Unexpected message size: %zd\n",
                   received);
            continue;
        }

        printf("\nMessage received!\n");
        printf("command = %d\n", msg.command);
        printf("value   = %d\n", msg.value);
    }

    return NULL;
}
//mqueue testing


	//SO - Train: CLEAR, Crossing: normal, Warning: OFF, Traffic: normal
	//S1 - Train: DETECTED, Crossing: train-crossing mode, Warning: ON, Traffic: red
	//S2 - fault in software or hardware or communication
int main(void)
{
	//testing mqueue
	struct mq_attr attr;
	mqd_t mq;
	memset(&attr, 0, sizeof(attr));

	attr.mq_maxmsg = 10;
	attr.mq_msgsize = sizeof(TrainTestMessage);

	mq = mq_open(
	    QUEUE_NAME,
	    O_RDONLY | O_CREAT,
	    0666,
	    &attr
	);

	if (mq == (mqd_t)-1)
	{
	    perror("mq_open");
	    return EXIT_FAILURE;
	}
	pthread_t mqThread;

	if (pthread_create(
	        &mqThread,
	        NULL,
	        mqReceiver,
	        &mq) != 0)
	{
	    perror("pthread_create");
	    return EXIT_FAILURE;
	}
	//testing mqueue


    TrainState currentState = S0_NORMAL;

    pthread_t inputThreadID;

    printf("TRAIN NODE STARTED\n");

    applyStateOutputs(currentState);

    if (pthread_create(&inputThreadID, NULL, inputThread, NULL) != 0)
    {
        perror("pthread_create");
        return EXIT_FAILURE;
    }

    while (1)
    {
        TrainEvent event = EVENT_NONE;

        pthread_mutex_lock(&eventMutex);

        event = pendingEvent;
        pendingEvent = EVENT_NONE;

        pthread_mutex_unlock(&eventMutex);

        if (event == EVENT_NONE)
        {
            continue;
        }

        if (event == EVENT_QUIT)
        {
            break;
        }

        TrainState previousState = currentState;

        processEvent(&currentState, event);

        if (currentState != previousState)
        {
            printf("\nState transition: %d -> %d\n",
                   previousState,
                   currentState);

            applyStateOutputs(currentState);
        }
        else
        {
            printf("No state transition.\n");
        }
    }

    pthread_join(inputThreadID, NULL);

    printf("Train Node terminated.\n");

    return EXIT_SUCCESS;
}


