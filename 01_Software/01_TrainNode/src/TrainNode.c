#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <errno.h>
#include <pthread.h>
#include <mqueue.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <string.h>
#include <unistd.h>
#include <mqueue.h>
#include <time.h>

#include "CommonMessages.h"

/* -------------------------------------------------- */
/* Node and queue configuration                       */
/* -------------------------------------------------- */

#define ID "X1"

/* The Control Node's outgoing queue must exist before T1 opens it. */
#define CONTROL_SEND_QUEUE "/net/Control_Node/Send_queue"

/* Replace this placeholder with the queue name confirmed by the team. */
#define T1_RECEIVE_QUEUE "/net/Control_Node/X1_rcv_queue"

#define EVENT_QUEUE_CAPACITY 32
#define STATUS_INTERVAL_SECONDS 1
#define RECEIVE_BUFFER_SIZE 1000

/* -------------------------------------------------- */
/* Train state machine                                 */
/* -------------------------------------------------- */

typedef enum {
    S0_NORMAL,
    S1_TRAIN_CROSSING,
    S2_FAILSAFE
} TrainState;

typedef enum {
    EVENT_NONE,
    EVENT_TRAIN_DETECTED,
    EVENT_CROSSING_CLEAR,
    EVENT_FAULT,
    EVENT_FAULT_CLEARED,
    EVENT_QUIT
} TrainEvent;

/* Shared message structures are defined in CommonMessages.h. */
static struct updateMsg uM;
static struct controlMsg cM;
static struct ForwardMsg fM; /* Reserved for the team's forwarding protocol. */
static struct replyMsg rM;

/* Queue descriptors are opened at runtime in initialiseComChannel(). */
static mqd_t mqs = (mqd_t)-1;
static mqd_t mqr = (mqd_t)-1;

/* Protects uM and lastStateChange between the state/status threads. */
static pthread_mutex_t statusUpdateMutex = PTHREAD_MUTEX_INITIALIZER;
static time_t lastStateChange;

/* Thread-safe event queue used by keyboard input and Control Node receiver. */
static TrainEvent eventQueue[EVENT_QUEUE_CAPACITY];
static int eventHead = 0;
static int eventTail = 0;
static int eventCount = 0;
static bool shutdownRequested = false; /* Access only while eventMutex is held. */
static pthread_mutex_t eventMutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t eventAvailable = PTHREAD_COND_INITIALIZER;

/* -------------------------------------------------- */
/* Send train status                                         */
/* -------------------------------------------------- */

static void sendTrainStatus(TrainState state)
{
    memset(&fM, 0, sizeof(fM));

    snprintf(fM.senderID, sizeof(fM.senderID), "%s", "T1");
    fM.currentState = (int)state;
    fM.trainApproach = (state == S1_TRAIN_CROSSING);

    /* Confirm the intended receiver with the Control Node developer. */
    snprintf(fM.RecevierID, sizeof(fM.RecevierID), "%s", "I2");

    if (mq_send(mqs, (const char *)&fM, sizeof(fM), 0) == -1) {
        perror("mq_send ForwardMsg");
    } else {
        printf("Train status sent to Control Node: state=%d, approaching=%d\n",
               fM.currentState, fM.trainApproach);
    }
}








/* -------------------------------------------------- */
/* Event queue                                         */
/* -------------------------------------------------- */

static int postEvent(TrainEvent event)
{
    pthread_mutex_lock(&eventMutex);

    if (shutdownRequested) {
        pthread_mutex_unlock(&eventMutex);
        return -1;
    }

    if (eventCount == EVENT_QUEUE_CAPACITY) {
        pthread_mutex_unlock(&eventMutex);
        fprintf(stderr, "Event queue full; event discarded\n");
        return -1;
    }

    eventQueue[eventTail] = event;
    eventTail = (eventTail + 1) % EVENT_QUEUE_CAPACITY;
    eventCount++;
    pthread_cond_signal(&eventAvailable);
    pthread_mutex_unlock(&eventMutex);
    return 0;
}

static TrainEvent waitForEvent(void)
{
    TrainEvent event;

    pthread_mutex_lock(&eventMutex);
    while (eventCount == 0 && !shutdownRequested) {
        pthread_cond_wait(&eventAvailable, &eventMutex);
    }

    if (eventCount == 0) {
        event = EVENT_QUIT;
    } else {
        event = eventQueue[eventHead];
        eventHead = (eventHead + 1) % EVENT_QUEUE_CAPACITY;
        eventCount--;
    }

    pthread_mutex_unlock(&eventMutex);
    return event;
}

static bool isShutdownRequested(void)
{
    bool stopping;
    pthread_mutex_lock(&eventMutex);
    stopping = shutdownRequested;
    pthread_mutex_unlock(&eventMutex);
    return stopping;
}

static void requestShutdown(void)
{
    pthread_mutex_lock(&eventMutex);
    shutdownRequested = true;
    pthread_cond_broadcast(&eventAvailable);
    pthread_mutex_unlock(&eventMutex);
}

/* -------------------------------------------------- */
/* State machine and simulated outputs                  */
/* -------------------------------------------------- */

static void applyStateOutputs(TrainState state)
{
    switch (state) {
    case S0_NORMAL:
        printf("\n=== S0: NORMAL ===\n");
        printf("Train: CLEAR\n");
        printf("Crossing: NORMAL\n");
        printf("Warning lights: OFF\n");
        printf("Traffic signal: NORMAL\n");
        sendTrainStatus(S0_NORMAL);
        break;

    case S1_TRAIN_CROSSING:
        printf("\n=== S1: TRAIN CROSSING ===\n");
        printf("Train: DETECTED\n");
        printf("Crossing: ACTIVE\n");
        printf("Warning lights: ON\n");
        printf("Traffic signal: RED\n");
        sendTrainStatus(S1_TRAIN_CROSSING);
        break;

    case S2_FAILSAFE:
        printf("\n=== S2: FAILSAFE ===\n");
        printf("FAULT CONDITION\n");
        printf("Train system: FAILSAFE\n");
        printf("Warning lights: FAILSAFE\n");
        printf("Traffic signal: FAILSAFE\n");
        sendTrainStatus(S2_FAILSAFE);
        break;
    }
}

static void processEvent(TrainState *state, TrainEvent event)
{
    switch (*state) {
    case S0_NORMAL:
        if (event == EVENT_TRAIN_DETECTED) {
            *state = S1_TRAIN_CROSSING;
        } else if (event == EVENT_FAULT) {
            *state = S2_FAILSAFE;
        }
        break;

    case S1_TRAIN_CROSSING:
        if (event == EVENT_CROSSING_CLEAR) {
            *state = S0_NORMAL;
        } else if (event == EVENT_FAULT) {
            *state = S2_FAILSAFE;
        }
        break;

    case S2_FAILSAFE:
        if (event == EVENT_FAULT_CLEARED) {
            *state = S0_NORMAL;
        }
        break;
    }
}

/* Update the status fields whenever an event is processed. */
static void updateStatusForEvent(TrainEvent event, TrainState state,
                                 TrainState previousState)
{
    pthread_mutex_lock(&statusUpdateMutex);

    if (event == EVENT_TRAIN_DETECTED) {
        uM.trainstate = true;
    } else if (event == EVENT_CROSSING_CLEAR) {
        uM.trainstate = false;
    }

    if (state != previousState) {
        lastStateChange = time(NULL);
        uM.currentState = (int)state;
    }

    /* The team has not yet specified a T1 mapping for phaseState. */
    //uM.phaseState = 0;
    uM.timeSinceLastChange = (int)(time(NULL) - lastStateChange);

    pthread_mutex_unlock(&statusUpdateMutex);
}

/* -------------------------------------------------- */
/* Keyboard input simulator                            */
/* -------------------------------------------------- */

static void *inputThread(void *arg)
{
    char input;
    (void)arg;

    while (!isShutdownRequested()) {
        printf("\nCommands:\n");
        printf("  A = Train approaching\n");
        printf("  C = Crossing clear\n");
        printf("  F = Fault\n");
        printf("  R = Fault cleared / reset\n");
        printf("  Q = Quit\n");
        printf("> ");
        fflush(stdout);

        if (scanf(" %c", &input) != 1) {
            postEvent(EVENT_QUIT);
            break;
        }

        switch (input) {
        case 'A':
        case 'a':
            postEvent(EVENT_TRAIN_DETECTED);
            break;
        case 'C':
        case 'c':
            postEvent(EVENT_CROSSING_CLEAR);
            break;
        case 'F':
        case 'f':
            postEvent(EVENT_FAULT);
            break;
        case 'R':
        case 'r':
            postEvent(EVENT_FAULT_CLEARED);
            break;
        case 'Q':
        case 'q':
            postEvent(EVENT_QUIT);
            return NULL;
        default:
            printf("Unknown command.\n");
            break;
        }
    }

    return NULL;
}

/* -------------------------------------------------- */
/* Send periodic status to the Control Node            */
/* -------------------------------------------------- */

static void *statusUpdate(void *arg)
{
    (void)arg;

    while (!isShutdownRequested()) {
        struct updateMsg snapshot;

        pthread_mutex_lock(&statusUpdateMutex);
        uM.timeSinceLastChange = (int)(time(NULL) - lastStateChange);
        snapshot = uM;
        pthread_mutex_unlock(&statusUpdateMutex);

        if (mq_send(mqs, (const char *)&snapshot, sizeof(snapshot), 0) == -1) {
            if (!isShutdownRequested()) {
                perror("mq_send status update");
            }
        }

        sleep(STATUS_INTERVAL_SECONDS);
    }

    return NULL;
}

/* -------------------------------------------------- */
/* Receive Control Node commands and acknowledgements  */
/* -------------------------------------------------- */

static void *controlReceive(void *arg)
{
    char buffer[RECEIVE_BUFFER_SIZE];
    (void)arg;

    while (!isShutdownRequested()) {
        ssize_t received = mq_receive(mqr, buffer, sizeof(buffer), NULL);

        if (received == -1) {
            if (errno == EINTR) {
                continue;
            }
            if (isShutdownRequested()) {
                break;
            }
            perror("mq_receive Control Node message");
            sleep(1);
            continue;
        }

        if ((size_t)received == sizeof(struct controlMsg)) {
            memcpy(&cM, buffer, sizeof(cM));
            cM.senderID[sizeof(cM.senderID) - 1] = '\0';

            printf("\nControl message received from %s\n", cM.senderID);
            if (cM.failState) {
                printf("Control Node requested FAILSAFE.\n");
                postEvent(EVENT_FAULT);
            } else {
                printf("Control Node failState is false; no automatic reset performed.\n");
            }
        } else if ((size_t)received == sizeof(struct replyMsg)) {
            memcpy(&rM, buffer, sizeof(rM));
            rM.senderID[sizeof(rM.senderID) - 1] = '\0';
            rM.buf[sizeof(rM.buf) - 1] = '\0';
            printf("Reply received from %s: %s\n", rM.senderID, rM.buf);
        } else {
            printf("Unexpected Control Node message size: %zd bytes\n", received);
        }
    }

    return NULL;
}

/* -------------------------------------------------- */
/* Communication setup                                 */
/* -------------------------------------------------- */

static int sendHandshake(const char *message)
{
    memset(&rM, 0, sizeof(rM));
    snprintf(rM.senderID, sizeof(rM.senderID), "%s", ID);
    snprintf(rM.buf, sizeof(rM.buf), "%s", message);

    if (mq_send(mqs, (const char *)&rM, sizeof(rM), 0) == -1) {
        perror("mq_send handshake");
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

static int initialiseComChannel(void)
{
    printf("Opening Control Node send queue: %s\n", CONTROL_SEND_QUEUE);
    mqs = mq_open(CONTROL_SEND_QUEUE, O_WRONLY);
    if (mqs == (mqd_t)-1) {
        perror("mq_open Control Node send queue");
        return EXIT_FAILURE;
    }

    printf("Opening T1 receive queue: %s\n", T1_RECEIVE_QUEUE);
    mqr = mq_open(T1_RECEIVE_QUEUE, O_RDONLY);
    if (mqr == (mqd_t)-1) {
        perror("mq_open T1 receive queue");
        mq_close(mqs);
        mqs = (mqd_t)-1;
        //return EXIT_FAILURE;
    }

    /* Keep these only if the Control Node expects the shared-template handshake. */
    if (sendHandshake("SEND_EOK") != EXIT_SUCCESS ||
        sendHandshake("RCV_EOK") != EXIT_SUCCESS) {
        mqs = (mqd_t)-1;
        mqr = (mqd_t)-1;
        //return EXIT_FAILURE;
    }

    printf("Communication queues opened; handshake messages sent.\n");
    return EXIT_SUCCESS;
}

/* -------------------------------------------------- */
/* Main                                                */
/* -------------------------------------------------- */

int main(void)
{
    pthread_t inputThreadID;
    pthread_t statusThreadID;
    pthread_t receiveThreadID;
    bool inputStarted = false;
    bool statusStarted = false;
    bool receiveStarted = false;
    TrainState currentState = S0_NORMAL;

    printf("TRAIN NODE %s STARTING\n", ID);

    memset(&uM, 0, sizeof(uM));
    snprintf(uM.senderID, sizeof(uM.senderID), "%s", ID);
    memset(&cM, 0, sizeof(cM));
    memset(&fM, 0, sizeof(fM));
    snprintf(fM.senderID, sizeof(fM.senderID), "%s", ID);
    memset(&rM, 0, sizeof(rM));

    pthread_mutex_lock(&statusUpdateMutex);
    lastStateChange = time(NULL);
    uM.currentState = (int)currentState;
    uM.trainstate = false;
    uM.timeSinceLastChange = 0;
    //uM.phaseState = 0;
    pthread_mutex_unlock(&statusUpdateMutex);

    if (initialiseComChannel() != EXIT_SUCCESS) {
        fprintf(stderr,
                "Communication initialisation failed. Confirm the queue paths.\n");
        return EXIT_FAILURE;
    }

    applyStateOutputs(currentState);

    if (pthread_create(&statusThreadID, NULL, statusUpdate, NULL) != 0) {
        fprintf(stderr, "Failed to create statusUpdate thread.\n");
        goto startup_failure;
    }
    statusStarted = true;

    if (pthread_create(&receiveThreadID, NULL, controlReceive, NULL) != 0) {
        fprintf(stderr, "Failed to create controlReceive thread.\n");
        goto startup_failure;
    }
    receiveStarted = true;

    if (pthread_create(&inputThreadID, NULL, inputThread, NULL) != 0) {
        fprintf(stderr, "Failed to create inputThread.\n");
        goto startup_failure;
    }
    inputStarted = true;

    while (1) {
        TrainEvent event = waitForEvent();
        TrainState previousState;

        if (event == EVENT_QUIT) {
            break;
        }

        previousState = currentState;
        processEvent(&currentState, event);
        updateStatusForEvent(event, currentState, previousState);

        if (currentState != previousState) {
            printf("\nState transition: %d -> %d\n",
                   (int)previousState, (int)currentState);
            applyStateOutputs(currentState);
        } else {
            printf("No state transition.\n");
        }
    }

    requestShutdown();
    if (inputStarted) {
        pthread_join(inputThreadID, NULL);
    }
    if (statusStarted) {
        pthread_join(statusThreadID, NULL);
    }
    if (receiveStarted) {
        pthread_cancel(receiveThreadID); /* Unblock a receiver waiting in mq_receive(). */
        pthread_join(receiveThreadID, NULL);
    }

    mq_close(mqs);
    mq_close(mqr);
    printf("Train Node terminated.\n");
    return EXIT_SUCCESS;

startup_failure:
    requestShutdown();
    if (inputStarted) {
        pthread_cancel(inputThreadID);
        pthread_join(inputThreadID, NULL);
    }
    if (statusStarted) {
        pthread_join(statusThreadID, NULL);
    }
    if (receiveStarted) {
        pthread_cancel(receiveThreadID);
        pthread_join(receiveThreadID, NULL);
    }
    if (mqs != (mqd_t)-1) {
        mq_close(mqs);
    }
    if (mqr != (mqd_t)-1) {
        mq_close(mqr);
    }
    return EXIT_FAILURE;
}
