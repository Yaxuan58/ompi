//
// Created by Yaxuan Wang on 13/11/19.
//
#include "ompi_config.h"

#include "mpi.h"
#include "ompi/constants.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/datatype/ompi_datatype_internal.h"
#include "ompi/op/op.h"
#include "ompi/mca/mca.h"
#include "opal/datatype/opal_convertor.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/request/request.h"
#include "ompi/communicator/communicator.h"
#include "ompi/mca/coll/base/base.h"
#include "ompi/datatype/ompi_datatype.h"

#include "coll_ipmulticast_bcast.h"
#include "coll_portals4.h"
// Yaxuan: additional headers, may be removable
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <sys/time.h>
#include <math.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <net/if.h>
#include <sys/ioctl.h>

#define IP_MULTICAST_PORT 12441 // 1024 and 65535.
#define IP_MULTICAST_ADDR "224.0.0.7"
//Yaxuan: not fixed. (origin 512)
#define MAX_BCAST_SIZE 512 // can be 1024 for "large" data

//Yaxuan: add more const below
#define PKT_NUM 30 // unsigned int mask for recieved packet
#define IP_ADDR_LEN 16

//for test
int packetIndex = 0;


typedef struct {
	bool is_root;
	bool needs_pack; 
	opal_convertor_t convertor; 

	char* data;
	size_t data_size; // In bytes

	// yx: additional variables
	int rank; // global
	int local_rank; // rank in subcommunicator , need to be changed to int[] for overlapping
    size_t chunk_size; // #pkt in each chunk =  MAX_BCAST_SIZE
    char IP[IP_ADDR_LEN];
    int udp_port;

} ompi_coll_ipmulticast_request_t;

//chunk msg type
typedef struct {
    unsigned int rank; // global rank
    unsigned int pkt_index; // pkt index

    unsigned int pkt_index;
    bool tail_chunk; // if this the last chunk

    char ip[IP_LEN];
} msg_t;

// ====================================================
// self-defined function (test if have time))

// setup parameters of reciever's socket
int init_recieve_socket(unsigned int multicast_port, char* multicast_addr, struct sockaddr_in *addr, unsigned int len) {
    int fd;
    fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        perror("Error: socket() in init_recieve_socket ");
        return -1;
    }
    memset(addr, 0, len);
    addr->sin_family = AF_INET;
    addr->sin_port = htons(multicast_port);
    addr->sin_addr.s_addr = htonl(INADDR_ANY);
    bool yes = true;
    // YW: setsockopt(sock, IPPROTO_IP, IP_MULTICAST_TTL, &ttl,sizeof(ttl))
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    if (bind(fd, (struct sockaddr*) addr, len) < 0) {
        perror("Error: bind() in init_recieve_socket");
        return -1;
    }
    // Request that the kernel join a multicast group
    struct ip_mreq mreq;
    mreq.imr_multiaddr.s_addr = inet_addr(multicast_addr);
    mreq.imr_interface.s_addr = htonl(INADDR_ANY);
    if (setsockopt(fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, (char*) &mreq, sizeof(mreq)) < 0) {
        perror("Error: setsockopt() in init_recieve_socket");
        return -1;
    }

    return fd;
}

// setup parameters of sender's socket

int init_send_socket(unsigned int multicast_port, char* multicast_addr, struct sockaddr_in *addr, unsigned int len) {
    int fd;
    char loopch = 0;
    fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        perror("Error: socket() in init_send_socket ");
        return -1;
    }
    memset(addr, 0, len);
    addr->sin_family = AF_INET;
    addr->sin_port = htons(multicast_port);
    addr->sin_addr.s_addr = inet_addr(multicast_addr);
    if (setsockopt(fd, IPPROTO_IP, IP_MULTICAST_LOOP, (char *)&loopch, sizeof(loopch)) < 0) {
        perror("Error: setsockopt() in init_send_socket ");
        close(fd);
        exit(1);
    }

    return fd;
}


int setup_send_udp_socket(unsigned int udp_port, struct sockaddr_in *addr, unsigned int len) {
    int fd;
    fd = socket(AF_INET, SOCK_DGRAM, 0);
    memset(addr, 0, len);
    addr->sin_addr.s_addr = htonl(INADDR_ANY);
    addr->sin_port = htons(udp_port);
    addr->sin_family = AF_INET;
    if(bind(fd, (struct sockaddr*)addr, len)) {
        perror("Error : Bind Failed");
        exit(0);
    }

    return fd;
}


int setup_recieve_udp_socket(unsigned int udp_port, char* udp_addr, struct sockaddr_in *addr, unsigned int len) {
    int fd;
    memset(addr, 0, len);
    fd = socket(AF_INET, SOCK_DGRAM, 0);
    addr->sin_addr.s_addr = inet_addr(udp_addr);
    addr->sin_port = htons(udp_port);
    addr->sin_family = AF_INET;
    //connect the endpoint of a previous socket descriptor
    //https://www.sciencedirect.com/topics/computer-science/struct-sockaddr
    if(connect(fd, (struct sockaddr *)addr, len) < 0) {
        perror("Error : Connect Failed");
        exit(0);
    }
    return fd;
}

//initial socket and get ip address
// interface = "eth0" (https://gist.github.com/wolfg1969/4572370)
static void get_IP_Addr(char *interface, char *ip_addr) {

    int fd;
    struct ifreq ifr;
    fd = socket(AF_INET, SOCK_DGRAM, 0);
    /* I want to get an IPv4 IP address */
    ifr.ifr_addr.sa_family = AF_INET;
    /* I want IP address attached to "eth0" */
    strncpy(ifr.ifr_name, "eth0", IFNAMSIZ-1);
    ioctl(fd, SIOCGIFADDR, &ifr);
    close(fd);

    strncpy(ip_addr, inet_ntoa(((struct sockaddr_in *)&ifr.ifr_addr)->sin_addr), IP_ADDR_LEN);
}

//initial socket and get ip address
// interface = "eth0" (https://gist.github.com/wolfg1969/4572370)
static void translate_rank (struct ompi_group_t *thisgroup, struct ompi_group_t *worldgroup,
                            struct ompi_communicator_t *comm, int* globalranks, int* localranks) {

    ompi_comm_group(comm, &thisgroup);
    ompi_comm_group(ompi_mpi_comm_world_addr, &worldgroup);
    int size = ompi_group_size(thisgroup);
    int *globalranks = malloc(size * sizeof(int));
    int *localranks = malloc(size * sizeof(int));
    for (int i = 0; i < size; i++) {
        localranks[i] = i;
    }
    ompi_group_translate_ranks(thisgroup, size, localranks, worldgroup, globalranks);
}

void set_time_limit(const int fd, int limit) {
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = limit;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof(tv));
}




// These next two functions are mostly taken from ompi/mca/coll/portals4/coll_portals4_bcast.c
// They handle serializing and deserializing in cases where that is non-trivial.
static int prepare_bcast_data (struct ompi_communicator_t *comm,
        void *buff, int count,
        struct ompi_datatype_t *datatype, int root,
        ompi_coll_ipmulticast_request_t *request) {
    int rank = ompi_comm_rank(comm);
    int ret;
    size_t max_data;
    unsigned int iov_count;
    struct iovec iovec;

    request->is_root = (rank == root);
    request->needs_pack = !ompi_datatype_is_contiguous_memory_layout(datatype, count);

    request->rank = rank;
    /* TODO for multi-communicators
     * translate_ranks(request->local_rank ...);
     *
     */

    // yaxuan: get IP address
    get_IP_Addr("eth0", request->ip);

	// Is this a special datatype that needs code to serialize and de-serialize it?
	if (request->needs_pack) {
		// If this is the root of the broadcast, we actually need to serialize the data now.
        if (request->is_root) {
            OBJ_CONSTRUCT(&request->convertor, opal_convertor_t);
            opal_convertor_copy_and_prepare_for_send(ompi_mpi_local_convertor,
                    &(datatype->super), count,
                    buff, 0, &request->convertor);
            opal_convertor_get_packed_size(&request->convertor, &request->data_size);
			// Allocate the buffer that we pack the data into
            request->data = malloc(request->data_size);
            if (OPAL_UNLIKELY(NULL == request->data)) {
                OBJ_DESTRUCT(&request->convertor);
                return opal_stderr("malloc failed", __FILE__, __LINE__, OMPI_ERR_OUT_OF_RESOURCE);
            }

            iovec.iov_base = request->data;
            iovec.iov_len = request->data_size;
            iov_count = 1;
            max_data = request->data_size;
            ret = opal_convertor_pack(&request->convertor, &iovec, &iov_count, &max_data);
            OBJ_DESTRUCT(&request->convertor);
            if (OPAL_UNLIKELY(ret < 0)) {
                return opal_stderr("opal_convertor_pack failed", __FILE__, __LINE__, ret);	}
        }
        else {
			// Construct the object converter to prepare for when we receive data
            OBJ_CONSTRUCT(&request->convertor, opal_convertor_t);
            opal_convertor_copy_and_prepare_for_recv(ompi_mpi_local_convertor,
                    &(datatype->super), count,
                    buff, 0, &request->convertor);

			// Philip's note: seems like the original code has a slight bug here.
            opal_convertor_get_packed_size(&request->convertor, &request->data_size);

            request->data = malloc(request->data_size);
            if (OPAL_UNLIKELY(NULL == request->data)) {
                OBJ_DESTRUCT(&request->convertor);
                return opal_stderr("malloc failed", __FILE__, __LINE__, OMPI_ERR_OUT_OF_RESOURCE);
            }
        }
    }
    else {
        request->data = buff;

		// Total size of message is (size of one element) * count
        ompi_datatype_type_size(datatype, &request->data_size);
        request->data_size *= count;

        // init chunk size
        request->chunk_size = MAX_BCAST_SIZE;

    }

    return (OMPI_SUCCESS);
}

static int post_bcast_data(	ompi_coll_ipmulticast_request_t *request) {

    int ret;
    size_t max_data;
    unsigned int iov_count;
    struct iovec iovec;

    if (request->needs_pack) {
        if (!request->is_root) {
			// We received data (since we're not the root) and need to de-serialize it into the right buffer
            opal_convertor_get_packed_size(&request->convertor, &request->data_size);

			// Convert the data we received to an iovec
            iovec.iov_base = request->data;
            iovec.iov_len = request->data_size;
            iov_count = 1;
            ret = opal_convertor_unpack(&request->convertor, &iovec, &iov_count, &max_data);
            OBJ_DESTRUCT(&request->convertor);
            if (OPAL_UNLIKELY(ret < 0)) {
                return opal_stderr("opal_convertor_unpack failed", __FILE__, __LINE__, ret);
            }
        }
		// This was a special buffer we allocated, so free it.
        free(request->data);
    }
    return (OMPI_SUCCESS);
}


int ompi_coll_ipmulticast_bcast(void *buff, int count,
        struct ompi_datatype_t *datatype, int root,
        struct ompi_communicator_t *comm,mca_coll_base_module_t *module) {

    //Yaxuan: initial all variables.
    int rank = ompi_comm_rank(comm);
    int ranks = ompi_comm_size(comm);
    packetIndex++;
    if (packetIndex%100==0) {
        printf("Rank_ %d calling custom bcast %d times\n",rank, packetIndex);
    }
	ompi_coll_ipmulticast_request_t request;
    prepare_bcast_data(comm, buff, count, datatype, root, &request);

    static int port_offset = 0;
    port_offset++;
    port_offset = port_offset % 60000;

    // TA: TODO: We don't need to create and destroy the socket every time
    int fd;
    int fd_udp;
    struct sockaddr_in src_addr, des_addr, src_addr_udp, des_addr_udp;
    int addr_len = sizeof(struct sockaddr_in);
    //YX: init msg
    msg_t = msg;
    msg.tail_chunk = false;
    msg.pkt_index = PKT_NUM;
    // build socket
    get_IP_Addr("eth0",msg.ip);
    int chunk_size = request.chunk_size;

    //TODO 
    unsigned int len;

	// If we're not sending, get ready to receive
	//setup multicast parameters
    if (!request.is_root) {
        fd = init_recieve_socket(IP_MULTICAST_PORT+port_offset, IP_MULTICAST_ADDR, &multi_daddr, addr_len);

        printf("Rank-Recv %d :  init_recieve_socket() ; root_rank %d  ;  ip %s ;  port %d\n", rank, root, IP_MULTICAST_ADDR, IP_MULTICAST_PORT+port_offset);

        set_socket_timeout(fd,1000000); //1M u second
    } else {
        fd = init_send_socket(IP_MULTICAST_PORT+port_offset, IP_MULTICAST_ADDR, &multi_saddr, addr_len);
        printf("Rank-Send %d :  init_send_socket() ; root_rank %d  ;  ip %s ;  port %d\n", rank, root, IP_MULTICAST_PORT+port_offset, IP_MULTICAST_ADDR);
    }

	// This kills performance but makes sure that all receivers are ready before the sender starts
    ompi_coll_portals4_barrier_intra(comm, module);

    ssize_t nbytes;
    if (request.is_root) {

        unsigned int port, len;
        port = rand() % 10000 + 5000; //port = (10000, 15000)
        request.udp_port = port;
        //setup udp server
        fd_udp = setup_send_udp_socket(port, &src_addr_udp, addr_len);

        // First send the size so that the receivers know how many messages to expect
        // TA: There are better ways to do this, no?

        int ranks_remaining; // remaining ranks
        int pkts_remaining; //remaining packets
        int this_pkt_size; // current packet size
        int this_chunk_size; // = MIN(size_remaining, chunk_size) ; original size of current chunk
        int nbytes_sum; // nbytes sent/recv in total
        int pkt_index; // index of current pkt in current chunk
        int offset;

        char* send_next_chunk = request.data; //send index for chunk
        char* send_next_pkt; //send index for pkt
        size_t size_remaining = request.data_size;
        nbytes = sendto(fd, &request.data_size, sizeof(size_t), 0, (struct sockaddr*) &addr, sizeof(addr));
        if (nbytes < 0)
            perror("Error: sendto() in ompi_coll_ipmulticast_bcast ");

        //start send a chunk
		while (size_remaining > 0) {
			// TA: UDP does not guarauntee ordering!!
            nbytes_sum = 0;
            this_pkt_size = this_chunk_size;
            pkts_remaining = PKT_NUM;
            send_next_pkt = send_next_chunk;
            this_chunk_size = MIN(size_remaining, chunk_size);

            nbytes = sendto(fd, send_next, MIN(size_remaining, MAX_BCAST_SIZE), 0, (struct sockaddr*) &addr, sizeof(addr));
            if (nbytes < 0)
                perror("sendto");

            // printf("Sent %zd\n", nbytes);
            size_remaining -= nbytes;
            send_next += nbytes;

            //start send a pkt
            while (pkts_remaining > 0 && size_remaining >0){
                printf("Rank-Send %d : remaining %d\n", rank, size_remaining);
                this_pkt_size = MIN(this_pkt_size, size_remaining);
                nbytes = sendto(fd, send_next_pkt, pkt_size, 0, (struct sockaddr*) &multi_saddr, addr_len);
                if (nbytes != pkt_size) {
                    perror("Rank-Send: sendto()");
                }
                // TODO


            }

            ranks_left = ranks - 1;
            while (ranks_remaining > 0) {
                // get message from pkt-loss reciever
                recvfrom(udp_fd, &msg, sizeof(msg), 0, (struct sockaddr*)&udp_daddr, &len);
                // TODO
		}
    }
	else {
		int addrlen = sizeof(addr);

		// Get the size
		nbytes = recvfrom(fd, &request.data_size, sizeof(size_t), 0, (struct sockaddr *) &addr, &addrlen);
		if (nbytes < 0)
			perror("Error: for size: recvfrom() in ompi_coll_ipmulticast_bcast  ");
		size_t size_remaining = request.data_size;
		// printf("Received %zd for size. Size is %zu\n", nbytes, size_remaining);
		char* recv_next = request.data;
		while (size_remaining > 0) {
			nbytes = recvfrom(fd, recv_next, MIN(size_remaining, MAX_BCAST_SIZE), 0, (struct sockaddr *) &addr, &addrlen);
			if (nbytes < 0)
				perror("Error: size_remaining > 0: recvfrom() in ompi_coll_ipmulticast_bcast");
			recv_next += nbytes;
			size_remaining -= nbytes;
			// printf("Received %zd\n", nbytes);
		}
	}
    close(fd);
	close(fd_udp);

	post_bcast_data(&request);
    return (OMPI_SUCCESS);
}

END_C_DECLS
