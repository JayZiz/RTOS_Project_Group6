#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <mqueue.h>
#include <sys/stat.h>
#include <string.h>
#include <stdint.h>

#include <time.h>
#include <sys/netmgr.h>
#include <sys/neutrino.h>
#include <errno.h>

#include <stdbool.h>
#include <stdint.h>
#include <ctype.h>

#include <hw/inout.h>      // for in32() and out32();
#include <sys/mman.h>      // for mmap_device_io();
#include <stdint.h>        // for unit32 types

#include <hw/i2c.h>

// ------------------------------------------ Definitions ------------------------------------------ //

#define keyboard_input 0

#define MSG_QUEUE					1000

#define Q_FLAGS 		O_RDWR | O_CREAT | O_EXCL
#define Q_Mode 			S_IRUSR | S_IWUSR
#define MY_PULSE_CODE 	_PULSE_CODE_MINAVAIL

#define replyBuf 					10
#define IDBuf 						10
#define receiverBuf 				10
#define ID 							"I2"

/* BeagleBone Black + XC4602/TTP229 16-key keypad (QNX). */
#define AM335X_CONTROL_MODULE_BASE  ((uint64_t)0x44E10000)
#define AM335X_CONTROL_MODULE_SIZE  ((size_t)0x00001448)
#define AM335X_GPIO_SIZE            ((size_t)0x00001000)
#define AM335X_GPIO1_BASE           ((uintptr_t)0x4804C000)
#define GPIO_OE                     0x134
#define GPIO_DATAIN                 0x138
#define GPIO_DATAOUT                0x13C
#define GPIO_IRQSTATUS_SET_1        0x38
#define GPIO_IRQWAKEN_1             0x48
#define GPIO_FALLINGDETECT          0x14C
#define GPIO_IRQSTATUS_1            0x30
#define GPIO1_IRQ                   99
#define P9_12_PIN_CONFIG            0x878
#define P9_15_PIN_CONFIG            0x840
#define SD0                         (1u << 28)
#define SCL                         (1u << 16)

/* BeagleBone Black + LCD screen (QNX). */
#define DATA_SEND					0x40
#define CLEAR_DISP					0x01
#define Co_Ctrl						0x00
#define LCD_STRING_LENGTH			21

// ------------------------------------------ Global Variables ------------------------------------------ //

const char *program_name = "03_Intersection2_Node.c";
const char * sendMqueueLocation = "/net/Control_Node/Send_queue";  			// one queue on control node for receiving data from this node
const char * rcvMqueueLocation = "/net/Control_Node/I2_rcv_queue"; 		// unique queue on control node for sending data to this node (xxx = node host name "need to discuss this")
char rcvBuffer[1000];														// receive message buffer

// ------------------------------------------ Structs ------------------------------------------ //

typedef struct{
	enum {
		STATE_ALL_RED,
		STATE_R3_GREEN,
		STATE_R3_YELLOW,
		STATE_R2_GREEN,
		STATE_R2_YELLOW,
		STATE_WAIT_TRAIN,
		STATE_FAIL
	}state_e;

	enum {
		PED_NONE,
		PED_W7_W8,
		PED_W9_W11
	}ped_e;

	enum {
		SENSOR_NONE,
		SENSOR_R2,
		SENSOR_R3
	}car_e;

	enum {
		TRAIN_NONE,
		TRAIN_PRESENT
	}train_e;

	enum {
		FAIL_NONE,
		FAIL_PRESENT
	}fail_e;

	struct {				// message that gets sent to control with current status of system
		char senderID[IDBuf];       // our data (unique id from client)
		int currentState;			// current state of the state machine
		int timeSinceLastChange;	// time since the state machine last changed state
		int carState;				// corresponding car event for each state
		int pedState;				// corresponding pedestrian event for each state
		bool trainstate;
	} updateMsg;

	struct {				// message received from control
		char senderID[IDBuf];       // control data (unique id)
		bool failState;				// force fail state
	} controlMsg ;

	struct {				// message received from control
		char senderID[IDBuf];       // control data (unique id)
		char RecevierID[IDBuf];		// where we are sending the message
		int currentState;			// current state of state machine
		bool trainApproach;			// train approaching or not
	} ForwardMsg;

	struct {				// the reply message for when a message is received from control
		char senderID[IDBuf];
	    char buf[replyBuf];			// Message we send back to clients to tell them the messages was processed correctly.
	} replyMsg;

	struct {
		int fd;
		uint8_t Address;
		uint8_t mode;
		char lcd_msg[LCD_STRING_LENGTH];
		bool lcd_msg_pending;
		pthread_mutex_t mutex;
	} LCD_connect;

	//QNX Message Queue parameters
	mqd_t mqs, mqr;

	// mutexes
	pthread_mutex_t statusUpdateMutex, stateUpdateMutex;
	pthread_mutex_t traffic_mutex;
	pthread_cond_t traffic_cond;

	//QNX Periodic Timer parameters
	int timer_chid;
	int timer_rcvid;
	timer_t timer_id;

	struct sigevent timer_evt;
	struct _pulse timer_pulse;
	struct itimerspec timer_green_spec, timer_yellow_spec, timer_pedes_spec;
	struct sched_param timer_param;

	//QNX Periodic Counter parameters
	int counter_chid;
	int counter_coid;
	timer_t counter_id;

	struct sigevent counter_evt;
	struct _pulse counter_pulse;
	struct itimerspec counter_spec;
	struct sched_param counter_param;

	//Fault flags
	bool sndConnectAlive;
	bool rcvConnectAlive;

}traffic_t;

typedef union _CONF_MODULE_PIN_STRUCT   // See TRM Page 1420
{
  unsigned int d32;
  struct {    // name: field size
           unsigned int conf_mmode : 3;       // LSB
           unsigned int conf_puden : 1;
           unsigned int conf_putypesel : 1;
           unsigned int conf_rxactive : 1;
           unsigned int conf_slewctrl : 1;
           unsigned int conf_res_1 : 13;      // reserved
           unsigned int conf_res_2 : 12;      // reserved MSB
         } b;
} _CONF_MODULE_PIN;

typedef struct
{
	int count_thread;
	uintptr_t gpio1_base;
	struct sigevent pevent; // remember to fill in "event" structure in main
}ISR_data;

// create global struct to share data between threads
ISR_data ISR_area_data;
//-------------------------Function prototypes---------------------------

void *check_input_control(void *data);
void *check_input_keypad(void *data);
void *check_input_keyboard(void *data);

void *write_output_lcd(void *data);

void *statusUpdate(void *data);

void *state_counter(void *data);

void initialise_timer(traffic_t *traffic_timer);
void initialise_counter(traffic_t *traffic_counter);

void update_state(traffic_t *traffic_state);
void I2_fsm_traffic(traffic_t *traffic_fsm);

int initialiseComChannel(traffic_t *traffic_comm);

//-----------------------------------------------------------------------

//-------------------------Check control input thread----------------------
void *check_input_control(void *data)
{
	traffic_t *node_traffic = (traffic_t*) data;
	char msg_recv[MSG_QUEUE];
	ssize_t bytes_recv;
	struct mq_attr q_attr;
	for(;;)
	{
		//Messages from the control node
		if((node_traffic->mqr = mq_open(rcvMqueueLocation, O_RDONLY)) != -1)
		{
			memset(msg_recv, 0, sizeof(msg_recv));
			mq_getattr(node_traffic->mqr, &q_attr);
			bytes_recv = mq_receive(node_traffic->mqr, msg_recv, MSG_QUEUE, NULL);
			//In case mq_recv gone wrong
			if(bytes_recv == -1)
			{
				perror("[I2] mq_receive issues");
				node_traffic->rcvConnectAlive = false;
				//exit(EXIT_FAILURE);
			}
			else
				node_traffic->rcvConnectAlive = true;
			pthread_mutex_lock(&node_traffic->traffic_mutex);
				//The messages obtained from the control node for FAIL state
				if((size_t)bytes_recv == sizeof(node_traffic->controlMsg))
				{
					memcpy(&node_traffic->controlMsg, msg_recv, sizeof(node_traffic->controlMsg));
					printf("[I2] controlMsg: sender=%s, failState=%d...\n",
							node_traffic->controlMsg.senderID,
							node_traffic->controlMsg.failState);
					if(node_traffic->controlMsg.failState == true)
					{
						printf("Received FAIL command from Control Node ...\n");
						node_traffic->fail_e = FAIL_PRESENT;
						node_traffic->state_e = STATE_FAIL;
						update_state(node_traffic);
					}
					else
					{
						printf("Received CLEAR_FAIL command from Control Node ...\n");
						node_traffic->fail_e = FAIL_NONE;
						node_traffic->state_e = STATE_ALL_RED;
						update_state(node_traffic);
					}
				}
				//The messages obtained from the control node for TRAIN state
				else if((size_t)bytes_recv == sizeof(node_traffic->ForwardMsg))
				{
					memcpy(&node_traffic->ForwardMsg, msg_recv, sizeof(node_traffic->ForwardMsg));
					printf("[I2] ForwardMsg: sender=%s, receiver=%s, currentState=%d, trainApproach=%d...\n",
							node_traffic->ForwardMsg.senderID,
							node_traffic->ForwardMsg.RecevierID,
							node_traffic->ForwardMsg.currentState,
							node_traffic->ForwardMsg.trainApproach);
					if(node_traffic->ForwardMsg.trainApproach == true)
					{
						printf("Received TRAIN_APPROACH command from Control Node ...\n");
						node_traffic->train_e = TRAIN_PRESENT;
						node_traffic->state_e = STATE_WAIT_TRAIN;
						update_state(node_traffic);
					}
					else
					{
						printf("Received CLEAR_TRAIN command from Control Node ...\n");
						node_traffic->train_e = TRAIN_NONE;
						node_traffic->state_e = STATE_ALL_RED;
						update_state(node_traffic);
					}
				}
			pthread_cond_signal(&node_traffic->traffic_cond);
			pthread_mutex_unlock(&node_traffic->traffic_mutex);
		}
		usleep(1);
	}
	mq_close(node_traffic->mqr);

	return EXIT_SUCCESS;
}

//-------------------------Check keypad input thread----------------------
static void keypad_delay(void)
{
    volatile int i;
    for (i = 0; i < 0x1F; ++i) { }
}

static void keypad_strobe(uintptr_t gpio)
{
    uint32_t value = in32(gpio + GPIO_DATAOUT);
    out32(gpio + GPIO_DATAOUT, value & ~SCL);
    keypad_delay();
    value = in32(gpio + GPIO_DATAOUT);
    out32(gpio + GPIO_DATAOUT, value | SCL);
    keypad_delay();
}

static int keypad_read_bit(uintptr_t gpio)
{
    return ((in32(gpio + GPIO_DATAIN) & SD0) == SD0) ? 1 : 0;
}

/*
 * Default keypad mapping:
 * 1 = vehicle demand R1; 3 = vehicle demand R3
 * 4 = pedestrian request W7-W8; 5 = pedestrian request W9-W11
 * 6 = train approaching; 7 = train cleared
 * 8 = clear vehicle and pedestrian requests
 *
 * Keys inject input requests; they do not directly force a green lamp.
 */
static void apply_keypad_key(traffic_t *keypad_traffic, uint32_t word)
{
    int key = 0;
    int i;

    for (i = 0; i < 16; ++i)
    {
        if (word == (1u << i))
        {
            key = i + 1;
            break;
        }
    }
    if (key == 0)
        return;

    pthread_mutex_lock(&keypad_traffic->traffic_mutex);
    switch (key)
    {
        case 2:
        	keypad_traffic->car_e = SENSOR_R2;
            printf("[KEYPAD] Vehicle request: R1\n");
            break;
        case 3:
        	keypad_traffic->car_e = SENSOR_R3;
            printf("[KEYPAD] Vehicle request: R3\n");
            break;
        case 4:
        	keypad_traffic->ped_e = PED_W7_W8;
            printf("[KEYPAD] Pedestrian request: W7-W8\n");
            break;
        case 5:
        	keypad_traffic->ped_e = PED_W9_W11;
            printf("[KEYPAD] Pedestrian request: W9-W11\n");
            break;
        case 6:
        	keypad_traffic->train_e = TRAIN_PRESENT;
            printf("[KEYPAD] Train approaching\n");
            break;
        case 7:
        	keypad_traffic->train_e = TRAIN_NONE;
            printf("[KEYPAD] Train cleared\n");
            break;
        case 8:
        	keypad_traffic->car_e = SENSOR_NONE;
        	keypad_traffic->ped_e = PED_NONE;
            printf("[KEYPAD] Vehicle and pedestrian requests cleared\n");
            break;
        default:
            printf("[KEYPAD] Key %d has no assigned action\n", key);
            break;
    }
    pthread_cond_signal(&keypad_traffic->traffic_cond);
    pthread_mutex_unlock(&keypad_traffic->traffic_mutex);
}


void *check_input_keypad(void *data)
{
	traffic_t *traffic = (traffic_t *)data;
	uintptr_t control = mmap_device_io(AM335X_CONTROL_MODULE_SIZE,
									   AM335X_CONTROL_MODULE_BASE);
	uintptr_t gpio = mmap_device_io(AM335X_GPIO_SIZE, AM335X_GPIO1_BASE);
	struct sigevent event;
	int interrupt_id;
	uint32_t value;
	int i;

	if (control == (uintptr_t)-1 || gpio == (uintptr_t)-1)
	{
		fprintf(stderr, "[KEYPAD] mmap_device_io failed\n");
		return NULL;
	}
	if (ThreadCtl(_NTO_TCTL_IO_PRIV, NULL) == -1)
	{
		perror("[KEYPAD] ThreadCtl");
		return NULL;
	}

	value = in32(gpio + GPIO_OE);
	value |= SD0;              // GPIO1_28 input
	value &= ~SCL;             // GPIO1_16 output
	out32(gpio + GPIO_OE, value);

	value = in32(gpio + GPIO_DATAOUT);
	out32(gpio + GPIO_DATAOUT, value | SCL);

	// P9_12: GPIO mode 7, input enabled, pull-up.
	value = 7u | (1u << 4) | (1u << 5) | (1u << 6);
	out32(control + P9_12_PIN_CONFIG, value);
	out32(control + P9_15_PIN_CONFIG, 7u); // P9_15: GPIO1_16 clock output

	out32(gpio + GPIO_IRQSTATUS_SET_1, SD0);
	out32(gpio + GPIO_IRQWAKEN_1, SD0);
	out32(gpio + GPIO_FALLINGDETECT, SD0);
	out32(gpio + GPIO_IRQSTATUS_1, SD0);

	memset(&event, 0, sizeof(event));
	event.sigev_notify = SIGEV_INTR;
	interrupt_id = InterruptAttachEvent(GPIO1_IRQ, &event,
										_NTO_INTR_FLAGS_TRK_MSK);
	if (interrupt_id == -1)
	{
		perror("[KEYPAD] InterruptAttachEvent");
		munmap_device_io(gpio, AM335X_GPIO_SIZE);
		munmap_device_io(control, AM335X_CONTROL_MODULE_SIZE);
		return NULL;
	}
	if (InterruptUnmask(GPIO1_IRQ, interrupt_id) == -1)
	{
		perror("[KEYPAD] InterruptUnmask");
		InterruptDetach(interrupt_id);
		munmap_device_io(gpio, AM335X_GPIO_SIZE);
		munmap_device_io(control, AM335X_CONTROL_MODULE_SIZE);
		return NULL;
	}

	printf("[KEYPAD] Ready: use keys 1-7 to inject traffic inputs.\n");
	for (;;)
	{
		if (InterruptWait(0, NULL) == -1)
		{
			if (errno == EINTR) continue;
			perror("[KEYPAD] InterruptWait");
			break;
		}

		InterruptDisable();
		value = 0;
		if (keypad_read_bit(gpio) == 0)
		{
			keypad_delay();
			for (i = 0; i < 16; ++i)
			{
				keypad_strobe(gpio);
				value |= ((uint32_t)(!keypad_read_bit(gpio)) << i);
			}
			apply_keypad_key(traffic, value);
		}

		out32(gpio + GPIO_IRQSTATUS_1, SD0);
		InterruptUnmask(GPIO1_IRQ, interrupt_id);
		InterruptEnable();
	}

	InterruptDetach(interrupt_id);
	munmap_device_io(gpio, AM335X_GPIO_SIZE);
	munmap_device_io(control, AM335X_CONTROL_MODULE_SIZE);
	return NULL;
}

//----------------------------------Check input keyboard thread----------------------------
void *check_input_keyboard(void *data)
{
    traffic_t *traffic = (traffic_t *)data;
    int key;

    printf("\n========== I2 Keyboard Test ==========\n");
    printf("1: Vehicle request R1\n");
    printf("3: Vehicle request R3\n");
    printf("4: Pedestrian request W1-W2\n");
    printf("5: Pedestrian request W3-W6\n");
    printf("6: Train approaching\n");
    printf("7: Train cleared\n");
    printf("8: Clear vehicle/pedestrian requests\n");
    printf("q: Quit keyboard thread\n");
    printf("======================================\n");

    while ((key = getchar()) != EOF)
    {
        if (key == '\n' || key == '\r')
            continue;

        if (key == 'q' || key == 'Q')
        {
            printf("[KEYBOARD] Keyboard thread stopped\n");
            break;
        }

        if (key >= '1' && key <= '8')
        {
            unsigned int word = 1u << (key - '1');
            printf("[KEYBOARD] Key %c pressed\n", key);
            apply_keypad_key(traffic, word);
        }
        else
        {
            printf("[KEYBOARD] Invalid key: %c\n", key);
        }
    }

    return NULL;
}

//-------------------------Check LCD output thread----------------------//
static int I2C_Write(int fd, uint8_t addr, uint8_t mode, uint8_t *pBuf, uint32_t data)
{
	i2c_send_t header;
	iov_t vectors[2];
	uint8_t packet[21];
	int status;

	/*
	 * packet[0] is the control byte.
	 * packet[1] to packet[20] hold LCD data.
	 */
	if (data > 20)
	{
		data = 20;
	}

	memset(packet, 0, sizeof(packet));

	packet[0] = mode;

	if (pBuf != NULL && data > 0)
	{
		memcpy(&packet[1], pBuf, data);
	}

	header.slave.addr = addr;
	header.slave.fmt = I2C_ADDRFMT_7BIT;
	header.len = data + 1;
	header.stop = 1;

	SETIOV(&vectors[0], &header, sizeof(header));
	SETIOV(&vectors[1], packet, data + 1);

	status = devctlv(fd,
					 DCMD_I2C_SEND,
					 2,
					 0,
					 vectors,
					 NULL,
					 NULL);

	if (status != EOK)
	{
		fprintf(stderr,
				"[LCD] I2C write failed: %s\n",
				strerror(status));
	}

	return status;
}

static void SetCursor(int fd, uint8_t LCDi2cAdd, uint8_t row, uint8_t column)
{
	uint8_t position = 0x80; // SET_DDRAM_CMD (control bit)
	uint8_t rowValue = 0;
	uint8_t	LCDcontrol = 0;
	if (row == 1)
		rowValue = 0x40;     // memory location offset for row 1
	position = (uint8_t)(position + rowValue + column);
	LCDcontrol = position;
	I2C_Write(fd, LCDi2cAdd, Co_Ctrl, &LCDcontrol, 1);		// write data to I2C
}

static void Initialise_LCD (int fd, _Uint32t LCDi2cAdd)
{
	uint8_t	LCDcontrol = 0x00;

	//   Initialise the LCD display via the I2C bus
	LCDcontrol = 0x38;  // data byte for FUNC_SET_TBL1
	I2C_Write(fd, LCDi2cAdd, Co_Ctrl, &LCDcontrol, 1);		// write data to I2C

	LCDcontrol = 0x39; // data byte for FUNC_SET_TBL2
	I2C_Write(fd, LCDi2cAdd, Co_Ctrl, &LCDcontrol, 1);		// write data to I2C

	LCDcontrol = 0x14; // data byte for Internal OSC frequency
	I2C_Write(fd, LCDi2cAdd, Co_Ctrl, &LCDcontrol, 1);		// write data to I2C

	LCDcontrol = 0x79; // data byte for contrast setting
	I2C_Write(fd, LCDi2cAdd, Co_Ctrl, &LCDcontrol, 1);		// write data to I2C

	LCDcontrol = 0x50; // data byte for Power/ICON control Contrast set
	I2C_Write(fd, LCDi2cAdd, Co_Ctrl, &LCDcontrol, 1);		// write data to I2C

	LCDcontrol = 0x6C; // data byte for Follower control
	I2C_Write(fd, LCDi2cAdd, Co_Ctrl, &LCDcontrol, 1);		// write data to I2C

	LCDcontrol = 0x0C; // data byte for Display ON
	I2C_Write(fd, LCDi2cAdd, Co_Ctrl, &LCDcontrol, 1);		// write data to I2C

	LCDcontrol = 0x01; // data byte for Clear display
	I2C_Write(fd, LCDi2cAdd, Co_Ctrl, &LCDcontrol, 1);		// write data to I2C
}

static void set_msg_lcd(traffic_t *traffic_lcd, const char *msg)
{
	pthread_mutex_lock(&traffic_lcd->LCD_connect.mutex);
		snprintf(traffic_lcd->LCD_connect.lcd_msg, LCD_STRING_LENGTH, "%s", msg);
		traffic_lcd->LCD_connect.lcd_msg_pending = true;
	pthread_mutex_unlock(&traffic_lcd->LCD_connect.mutex);
}

void *write_output_lcd(void *data)
{
	traffic_t *traffic_lcd = (traffic_t*)data;
	char LCD_data[LCD_STRING_LENGTH];

	int file;
	int error;
	volatile uint8_t LCDi2cAdd = 0x3C;
	uint32_t speed = 200000; // nice and slow (will work with 200000)

	pthread_mutex_init(&traffic_lcd->LCD_connect.mutex, NULL);

	// Open I2C resource and set it up
	if ((file = open("/dev/i2c1",O_RDWR)) < 0)	  // OPEN I2C1
		printf("Error while opening Device File.!!\n");
	else
		printf("I2C1 Opened Successfully\n");

	error = devctl(file,DCMD_I2C_SET_BUS_SPEED,&(speed),sizeof(speed),NULL);  // Set Bus speed
	if (error)
	{
		fprintf(stderr, "Error setting the bus speed: %d\n",strerror ( error ));
		exit(EXIT_FAILURE);
	}
	else
		printf("Bus speed set = %d\n", speed);

	Initialise_LCD(file, LCDi2cAdd);

	usleep(1);

	pthread_mutex_lock(&traffic_lcd->LCD_connect.mutex);
		traffic_lcd->LCD_connect.fd = file;
		traffic_lcd->LCD_connect.Address = LCDi2cAdd;
		traffic_lcd->LCD_connect.mode = DATA_SEND;
	pthread_mutex_unlock(&traffic_lcd->LCD_connect.mutex);

	for(;;)
	{
		pthread_mutex_lock(&traffic_lcd->LCD_connect.mutex);
		if(traffic_lcd->LCD_connect.lcd_msg_pending)
		{
			//Copy and print new string
			strncpy(LCD_data, traffic_lcd->LCD_connect.lcd_msg, sizeof(LCD_data)-1);
			LCD_data[sizeof(LCD_data) - 1] = '\0';
			SetCursor(traffic_lcd->LCD_connect.fd,
					traffic_lcd->LCD_connect.Address, 0, 0); //First row
			I2C_Write(traffic_lcd->LCD_connect.fd,
					traffic_lcd->LCD_connect.Address, DATA_SEND, (uint8_t*)LCD_data, strlen(LCD_data));
			traffic_lcd->LCD_connect.lcd_msg_pending = false;
		}
		pthread_mutex_unlock(&traffic_lcd->LCD_connect.mutex);
		usleep(100000);
	}
	return NULL;
}

// ############## Status Update Thread ############## //

void *statusUpdate(void *data)
{
	// might update to have higher priority later
	traffic_t *traffic_status = (traffic_t*) data;
	int count_fail = 0;
	while(traffic_status->sndConnectAlive && traffic_status->rcvConnectAlive)
	{
		sleep(1);										// sleep for 1 second, i will replace with HW timer later
		pthread_mutex_lock(&traffic_status->statusUpdateMutex);				// lock for status update struct use (maybe change to add time limit later)
		// send status update to co// send status update to control node
		if(mq_send(traffic_status->mqs, (char *)&traffic_status->updateMsg, sizeof(traffic_status->updateMsg), 0) == -1)
		{
			count_fail++;
		}
		else
		{
			count_fail = 0;
		}
		pthread_mutex_unlock(&traffic_status->statusUpdateMutex);			// unlock to allow writing to status update struct by other threads

		if(count_fail >= 3)
		{
			traffic_status->sndConnectAlive = false;
		}
		else
			traffic_status->sndConnectAlive = true;
	}
	return EXIT_SUCCESS;
}

//-----------------------------State counter thread---------------------------
void *state_counter(void *data)
{
	traffic_t *traffic_update = (traffic_t*) data;
	int rcv_id;
	for(;;)
	{
		rcv_id = MsgReceive(traffic_update->counter_chid, &traffic_update->counter_pulse,
		            sizeof(traffic_update->counter_pulse), NULL);
		if (rcv_id == 0)
		{
			if (traffic_update->counter_pulse.code ==
					MY_PULSE_CODE)
			{
				pthread_mutex_lock(&traffic_update->stateUpdateMutex);

				traffic_update->updateMsg.timeSinceLastChange++;

				pthread_mutex_unlock(&traffic_update->stateUpdateMutex);
			}
		}
		else if (rcv_id == -1)
		{
			perror("MsgReceive counter");
			exit(EXIT_FAILURE);
		}
	}
	return EXIT_SUCCESS;
}

void initialise_timer(traffic_t *traffic_timer)
{
	//Create a channel
	traffic_timer->timer_chid = ChannelCreate(0);
	traffic_timer->timer_evt.sigev_notify = SIGEV_PULSE;

	//Create a connection back to ourselves for the timer to send the pulse on
	traffic_timer->timer_evt.sigev_coid = ConnectAttach(ND_LOCAL_NODE, 0, traffic_timer->timer_chid, _NTO_SIDE_CHANNEL, 0);
	if (traffic_timer->timer_evt.sigev_coid == -1)
	{
	   printf(stderr, "%s:  couldn't ConnectAttach to self!\n", program_name);
	   perror(NULL);
	   exit(EXIT_FAILURE);
	}
	pthread_getschedparam(pthread_self(), NULL, &traffic_timer->timer_param);
	traffic_timer->timer_evt.sigev_priority = traffic_timer->timer_param.sched_curpriority;    // old QNX660 version getprio(0);
	traffic_timer->timer_evt.sigev_code = MY_PULSE_CODE;

	//Create the timer then bind it to the event
	if (timer_create(CLOCK_REALTIME, &traffic_timer->timer_evt, &traffic_timer->timer_id) == -1)
	{
	   printf(stderr, "%s:  couldn't create a timer, errno %d\n", program_name, errno);
	   perror (NULL);
	   exit(EXIT_FAILURE);
	}

	//Set timer for green
	traffic_timer->timer_green_spec.it_value.tv_sec = 10;
	traffic_timer->timer_green_spec.it_value.tv_nsec = 0;
	traffic_timer->timer_green_spec.it_interval.tv_sec = 0; //one shot
	traffic_timer->timer_green_spec.it_interval.tv_nsec = 0;
	//Set timer for yellow
	traffic_timer->timer_yellow_spec.it_value.tv_sec = 5;
	traffic_timer->timer_yellow_spec.it_value.tv_nsec = 0;
	traffic_timer->timer_yellow_spec.it_interval.tv_sec = 0; //one shot
	traffic_timer->timer_yellow_spec.it_interval.tv_nsec = 0;
	//Set timer for pedestrian
	traffic_timer->timer_pedes_spec.it_value.tv_sec = 5;
	traffic_timer->timer_pedes_spec.it_value.tv_nsec = 0;
	traffic_timer->timer_pedes_spec.it_interval.tv_sec = 0; //one shot
	traffic_timer->timer_pedes_spec.it_interval.tv_nsec = 0;
}

void initialise_counter(traffic_t *traffic_counter)
{
	//Create a channel
	traffic_counter->counter_chid = ChannelCreate(0);
	traffic_counter->counter_evt.sigev_notify = SIGEV_PULSE;

	//Create a connection back to ourselves for the timer to send the pulse on
	traffic_counter->counter_evt.sigev_coid = ConnectAttach(ND_LOCAL_NODE, 0, traffic_counter->counter_chid, _NTO_SIDE_CHANNEL, 0);
	if (traffic_counter->counter_evt.sigev_coid == -1)
	{
	   printf(stderr, "%s:  couldn't ConnectAttach to self!\n", program_name);
	   perror(NULL);
	   exit(EXIT_FAILURE);
	}
	pthread_getschedparam(pthread_self(), NULL, &traffic_counter->counter_param);
	traffic_counter->counter_evt.sigev_priority = traffic_counter->counter_param.sched_curpriority;    // old QNX660 version getprio(0);
	traffic_counter->counter_evt.sigev_code = MY_PULSE_CODE;

	//Create the timer then bind it to the event
	if (timer_create(CLOCK_REALTIME, &traffic_counter->counter_evt, &traffic_counter->counter_id) == -1)
	{
	   printf(stderr, "%s:  couldn't create a timer, errno %d\n", program_name, errno);
	   perror (NULL);
	   exit (EXIT_FAILURE);
	}

	//Set timer for state counter
	traffic_counter->counter_spec.it_value.tv_sec = 1;
	traffic_counter->counter_spec.it_value.tv_nsec = 0;
	traffic_counter->counter_spec.it_interval.tv_sec = 1;
	traffic_counter->counter_spec.it_interval.tv_nsec = 0;

	//Start counter
	if(timer_settime(traffic_counter->counter_id, 0,
			&traffic_counter->counter_spec, NULL) == -1)
	{
		perror(NULL);
		exit(EXIT_FAILURE);
	}
}

void update_state(traffic_t *traffic_state)
{
	pthread_mutex_lock(&traffic_state->stateUpdateMutex);

	traffic_state->updateMsg.currentState = traffic_state->state_e;
	traffic_state->updateMsg.carState = traffic_state->car_e;
	traffic_state->updateMsg.pedState = traffic_state->ped_e;
	traffic_state->updateMsg.trainstate = traffic_state->train_e;
	traffic_state->updateMsg.timeSinceLastChange = 0;

	pthread_mutex_unlock(&traffic_state->stateUpdateMutex);
}

// ######################################################################## //
// ############## Establish communications with control Node ############## //
// ######################################################################## //
int initialiseComChannel(traffic_t *traffic_comm)
{
	// using mqueue as the minimum, upgrade to native message passing if time
	// just really an initial check to see if queue exists before starting the system up
	// check connection to send queue
	printf("Connecting to %s queue on control node, please wait\n" ,sendMqueueLocation);
	int mqueueEstablished = 0;														// for while loop
	strlcpy(traffic_comm->replyMsg.buf, "SEND_EOK", sizeof(traffic_comm->replyMsg.buf));									// confirmation message for send queue
	while(!mqueueEstablished){														// loop until mqueue is opened successfully (need a timeout???)
		traffic_comm->mqs = mq_open(sendMqueueLocation, O_WRONLY);								// opens queue on control node if it exists
		if (traffic_comm->mqs != -1){																// if connection is good
			if (mq_send(traffic_comm->mqs,(const char*)&traffic_comm->replyMsg, sizeof(traffic_comm->replyMsg), 0) == -1){
				perror("mq_send");
				traffic_comm->sndConnectAlive = false;
				exit(EXIT_FAILURE);
			}						// send confirmation to control (do we need to check control get this msg lmao)
			printf("-->connection to %s on control node confirmed!\n", sendMqueueLocation);
			mqueueEstablished = 1;
			traffic_comm->sndConnectAlive = true;
		} else {																	// else no connection
			//printf(">");															// just to show system is not frozen.
		}
		//sleep(1);																	// update to hw timer maybe (probably doesnt matter at this stage in the code)
	}

	// check connection to rcv queue
	printf("\nConnecting to %s queue on control node, please wait\n", rcvMqueueLocation);
	mqueueEstablished = 0;															// reset while loop variable
	strlcpy(traffic_comm->replyMsg.buf, "RCV_EOK", sizeof(traffic_comm->replyMsg.buf));										// confirmation message for send queue
	while(!mqueueEstablished){														// loop until mqueue is opened successfully (need a timeout???)
		traffic_comm->mqr = mq_open(rcvMqueueLocation, O_RDONLY);									// opens queue on control node if it exists
		if (traffic_comm->mqr != -1){																// if connection is good
			if (mq_send(traffic_comm->mqs,(const char*)&traffic_comm->replyMsg, sizeof(traffic_comm->replyMsg), 0) == -1){				// send confirmation to control (should we pull data from queue to confirm working?)
				perror("mq_send");
				traffic_comm->rcvConnectAlive = false;
				exit(EXIT_FAILURE);
			}
			printf("-->connection to %s on control node confirmed!\n", rcvMqueueLocation);
			mqueueEstablished = 1;
			traffic_comm->rcvConnectAlive = true;
		} else {																	// else no connection
			//printf(">");															// just to show system is not frozen. implement another time, keeps glitching out
		}
		//sleep(1);																	// update to hw timer maybe (probably doesnt matter at this stage in the code)
	}
	// note that the queues remain open so you don't need to use mq_open just mq_send/receive when sending or reading messages.
	return EXIT_SUCCESS;
}

static int wait_for_timer(traffic_t *traffic, const struct itimerspec *spec)
{
	struct itimerspec stop;
	struct _pulse pulse;
	uint64_t flush_timeout_ns = 1000000;    /* 1 ms: only paid once, when the queue is empty */

	// 1. Disarm: make sure the previous arming can't fire any more
	stop.it_value.tv_sec = 0;
	stop.it_value.tv_nsec = 0;
	stop.it_interval.tv_sec = 0;
	stop.it_interval.tv_nsec = 0;
	timer_settime(traffic->timer_id, 0, &stop, NULL);

	// 2. Flush stale pulses already queued on the channel
	for (;;)
	{
		TimerTimeout(CLOCK_MONOTONIC, _NTO_TIMEOUT_RECEIVE, NULL, &flush_timeout_ns, NULL);
		if (MsgReceivePulse(traffic->timer_chid, &pulse, sizeof(pulse), NULL) == -1)
		{
			break;      /* ETIMEDOUT -> channel is empty */
		}
	}

	// 3. Arm the new one-shot timer
	if (timer_settime(traffic->timer_id, 0, spec, NULL) == -1)
	{
		perror("timer_settime");
		return -1;
	}

	// 4. Wait for exactly one fresh pulse
	for (;;)
	{
		int rcvid = MsgReceive(traffic->timer_chid, &pulse, sizeof(pulse), NULL);
		if (rcvid == 0 && pulse.code == MY_PULSE_CODE)
		{
			return 0;
		}
		if (rcvid == -1 && errno != EINTR)
		{
			perror("MsgReceive(timer)");
			return -1;
		}
		/* anything else: ignore and keep waiting */
	}
}

void I2_fsm_traffic(traffic_t *traffic_fsm)
{
	switch(traffic_fsm->state_e)
	{
	case STATE_ALL_RED:
		set_msg_lcd(traffic_fsm, "I2 ALL RED state");
		update_state(traffic_fsm);
		pthread_mutex_lock(&traffic_fsm->traffic_mutex);

		while(traffic_fsm->car_e == SENSOR_NONE &&
			  traffic_fsm->ped_e == PED_NONE &&
			  traffic_fsm->train_e == TRAIN_NONE &&
			  traffic_fsm->fail_e == FAIL_NONE)
		{
			pthread_cond_wait(&traffic_fsm->traffic_cond,
							  &traffic_fsm->traffic_mutex);
		}

		pthread_mutex_unlock(&traffic_fsm->traffic_mutex);

		if(traffic_fsm->car_e == SENSOR_R2 || traffic_fsm->ped_e == PED_W9_W11)
		{
			set_msg_lcd(traffic_fsm, "TransitR2GreenIn30s");
			//Start the timer!
			if(wait_for_timer(traffic_fsm, &traffic_fsm->timer_green_spec) == 0)
			{
				traffic_fsm->state_e = STATE_R2_GREEN;
				update_state(traffic_fsm);
			}
		}

		else if(traffic_fsm->car_e == SENSOR_R3 || traffic_fsm->ped_e == PED_W7_W8)
		{
			set_msg_lcd(traffic_fsm, "TransitR3GreenIn30s");
			//Start the timer!
			if(wait_for_timer(traffic_fsm, &traffic_fsm->timer_green_spec) == 0)
			{
				traffic_fsm->state_e = STATE_R3_GREEN;
				update_state(traffic_fsm);
			}
		}

		if(traffic_fsm->train_e == TRAIN_PRESENT)
		{
			set_msg_lcd(traffic_fsm, "TransitI2WaitTrain");
			traffic_fsm->state_e = STATE_WAIT_TRAIN;
			update_state(traffic_fsm);
		}

		if((traffic_fsm->rcvConnectAlive) && (traffic_fsm->sndConnectAlive) != 1)
		{
			set_msg_lcd(traffic_fsm, "TransitI2Fail");
			traffic_fsm->state_e = STATE_FAIL;
			update_state(traffic_fsm);
		}

		break;
	case STATE_R3_GREEN:
		set_msg_lcd(traffic_fsm, "I2-R3 GREEN state");
		pthread_mutex_lock(&traffic_fsm->traffic_mutex);
		//Remain in this state until an event comes
		while(traffic_fsm->car_e != SENSOR_R2 &&
			  traffic_fsm->ped_e != PED_W9_W11 &&
			  traffic_fsm->train_e != TRAIN_PRESENT)
		{
			pthread_cond_wait(&traffic_fsm->traffic_cond,
							  &traffic_fsm->traffic_mutex);
		}

		pthread_mutex_unlock(&traffic_fsm->traffic_mutex);

		if(traffic_fsm->car_e == SENSOR_R2 || traffic_fsm->ped_e == PED_W9_W11)
		{
			set_msg_lcd(traffic_fsm, "TransitR3YellowIn20s");
			//Start the timer!
			if(wait_for_timer(traffic_fsm, &traffic_fsm->timer_yellow_spec) == 0)
			{
				traffic_fsm->state_e = STATE_R3_YELLOW;
				update_state(traffic_fsm);
			}
		}

		if(traffic_fsm->train_e == TRAIN_PRESENT)
		{
			set_msg_lcd(traffic_fsm, "TransitI2WaitTrain");
			traffic_fsm->state_e = STATE_WAIT_TRAIN;
			update_state(traffic_fsm);
		}

		if((traffic_fsm->rcvConnectAlive) && (traffic_fsm->sndConnectAlive) != 1)
		{
			set_msg_lcd(traffic_fsm, "TransitI2Fail");
			traffic_fsm->state_e = STATE_FAIL;
			update_state(traffic_fsm);
		}
		break;
	case STATE_R3_YELLOW:
		set_msg_lcd(traffic_fsm, "I2-R3 YELLOW state");
		set_msg_lcd(traffic_fsm, "TransitR2GreenIn10s");
		//Start the timer
		if(wait_for_timer(traffic_fsm, &traffic_fsm->timer_pedes_spec) == 0)
		{
			traffic_fsm->state_e = STATE_R2_GREEN;
			update_state(traffic_fsm);
		}

		if(traffic_fsm->train_e == TRAIN_PRESENT)
		{
			set_msg_lcd(traffic_fsm, "TransitI2WaitTrain");
			traffic_fsm->state_e = STATE_WAIT_TRAIN;
			update_state(traffic_fsm);
		}

		if((traffic_fsm->rcvConnectAlive) && (traffic_fsm->sndConnectAlive) != 1)
		{
			set_msg_lcd(traffic_fsm, "TransitI2Fail");
			traffic_fsm->state_e = STATE_FAIL;
			update_state(traffic_fsm);
		}
		break;
	case STATE_R2_GREEN:
		set_msg_lcd(traffic_fsm, "I2-R2 GREEN state");
		pthread_mutex_lock(&traffic_fsm->traffic_mutex);
		//Remain in this state until an event comes
		while(traffic_fsm->car_e != SENSOR_R3 &&
			  traffic_fsm->ped_e != PED_W7_W8 &&
			  traffic_fsm->train_e != TRAIN_PRESENT)
		{
			pthread_cond_wait(&traffic_fsm->traffic_cond,
							  &traffic_fsm->traffic_mutex);
		}

		pthread_mutex_unlock(&traffic_fsm->traffic_mutex);

		if(traffic_fsm->car_e == SENSOR_R3 || traffic_fsm->ped_e == PED_W7_W8)
		{
			set_msg_lcd(traffic_fsm, "TransitR2YellowIn20s");
			//Start the timer!
			if(wait_for_timer(traffic_fsm, &traffic_fsm->timer_yellow_spec) == 0)
			{
				traffic_fsm->state_e = STATE_R2_YELLOW;
				update_state(traffic_fsm);
			}
		}

		if(traffic_fsm->train_e == TRAIN_PRESENT)
		{
			set_msg_lcd(traffic_fsm, "TransitI2WaitTrain");
			traffic_fsm->state_e = STATE_WAIT_TRAIN;
			update_state(traffic_fsm);
		}

		if((traffic_fsm->rcvConnectAlive) && (traffic_fsm->sndConnectAlive) != 1)
		{
			set_msg_lcd(traffic_fsm, "TransitI2Fail");
			traffic_fsm->state_e = STATE_FAIL;
			update_state(traffic_fsm);
		}
		break;
	case STATE_R2_YELLOW:
		set_msg_lcd(traffic_fsm, "I2-R2 YELLOW state");
		set_msg_lcd(traffic_fsm, "TransitR3GreenIn10s");
		//Start the timer!
		if(wait_for_timer(traffic_fsm, &traffic_fsm->timer_pedes_spec) == 0)
		{
			traffic_fsm->state_e = STATE_R3_GREEN;
			update_state(traffic_fsm);
		}

		if(traffic_fsm->train_e == TRAIN_PRESENT)
		{
			set_msg_lcd(traffic_fsm, "TransitI2WaitTrain");
			traffic_fsm->state_e = STATE_WAIT_TRAIN;
			update_state(traffic_fsm);
		}

		if((traffic_fsm->rcvConnectAlive) && (traffic_fsm->sndConnectAlive) != 1)
		{
			set_msg_lcd(traffic_fsm, "TransitI2Fail");
			traffic_fsm->state_e = STATE_FAIL;
			update_state(traffic_fsm);
		}
		break;
	case STATE_WAIT_TRAIN:
		set_msg_lcd(traffic_fsm, "I2: Railway is closed");
		if(traffic_fsm->train_e == TRAIN_NONE)
		{
			traffic_fsm->state_e = STATE_ALL_RED;
			update_state(traffic_fsm);
		}
		sleep(5);
		break;
	case STATE_FAIL:
		printf("I2: Connection lost");
		sleep(5);
		break;
	default:
		printf("Unknown FSM state, exiting now ...\n");
		break;
	}
}

int main(int argc, char *argv[])
{
	printf("Intersection I2 Node in operation ...\n");

	//--------------------Traffic part-------------------------//
	traffic_t I2_traffic = {
			.state_e = STATE_ALL_RED,
			.car_e = SENSOR_NONE,
			.ped_e = PED_NONE,
			.train_e = TRAIN_NONE,
			.traffic_mutex = PTHREAD_MUTEX_INITIALIZER,
			.traffic_cond = PTHREAD_COND_INITIALIZER,
			.statusUpdateMutex = PTHREAD_MUTEX_INITIALIZER,
			.stateUpdateMutex = PTHREAD_MUTEX_INITIALIZER,
			.sndConnectAlive = false,
			.rcvConnectAlive = false
	};

	memset(&I2_traffic.updateMsg, 0, sizeof(I2_traffic.updateMsg)); 						// Clears the array
	strlcpy(I2_traffic.updateMsg.senderID, ID, sizeof(I2_traffic.updateMsg.senderID));		// copies string into struct element
		// reply message
	memset(&I2_traffic.replyMsg, 0, sizeof(I2_traffic.replyMsg)); 						// Clears the array
	strlcpy(I2_traffic.replyMsg.senderID, ID, sizeof(I2_traffic.replyMsg.senderID));		// copies string into struct element
		// forward message
	memset(&I2_traffic.ForwardMsg, 0, sizeof(I2_traffic.ForwardMsg)); 						// Clears the array
	strlcpy(I2_traffic.ForwardMsg.senderID, ID, sizeof(I2_traffic.ForwardMsg.senderID));		// copies string into struct element
		// control message
	memset(&I2_traffic.controlMsg, 0, sizeof(I2_traffic.controlMsg)); 						// Clears the array
	strlcpy(I2_traffic.controlMsg.senderID, ID, sizeof(I2_traffic.controlMsg.senderID));		// copies string into struct element

	void *retVal;

	// check communication channel
	//initialiseComChannel(&I2_traffic);
	initialise_timer(&I2_traffic);
	initialise_counter(&I2_traffic);

	// initialise threads (default for now maybe change to round robin later if time)
	pthread_t su;
	pthread_create(&su,NULL,statusUpdate, &I2_traffic);

	pthread_t input_control_thread, input_keypad_thread;
	pthread_create(&input_control_thread, NULL, check_input_control, &I2_traffic);
#if keyboard_input
	pthread_create(&input_keypad_thread, NULL, check_input_keyboard, &I2_traffic);
#else
	pthread_create(&input_keypad_thread, NULL, check_input_keypad, &I2_traffic);
#endif

	pthread_t output_lcd_thread;
	pthread_create(&output_lcd_thread, NULL, write_output_lcd, &I2_traffic);

	pthread_mutex_init(&I2_traffic.traffic_mutex, NULL);
	pthread_mutex_init(&I2_traffic.statusUpdateMutex, NULL);
	pthread_mutex_init(&I2_traffic.stateUpdateMutex, NULL);

	for(;;)
	{
		I2_fsm_traffic(&I2_traffic);
		usleep(1);
	}

	pthread_join(su, &retVal);
	pthread_join(input_control_thread, &retVal);
	pthread_join(input_keypad_thread, &retVal);
	pthread_join(output_lcd_thread, &retVal);

	pthread_mutex_destroy(&I2_traffic.traffic_mutex);
	pthread_mutex_destroy(&I2_traffic.stateUpdateMutex);
	pthread_mutex_destroy(&I2_traffic.statusUpdateMutex);

	return EXIT_SUCCESS;
}
