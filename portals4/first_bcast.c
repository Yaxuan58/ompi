
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


#define IP_MULTICAST_PORT 12441
#define IP_MULTICAST_ADDR "224.0.0.7"
#define MAX_BCAST_SIZE 512


typedef struct {
    bool is_root;
    bool needs_pack;
    opal_convertor_t convertor;

    char* data;
    size_t data_size; // In bytes
} ompi_coll_ipmulticast_request_t;

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
    printf("Calling custom bcast\n");

    ompi_coll_ipmulticast_request_t request;
    prepare_bcast_data(comm, buff, count, datatype, root, &request);

    // TODO: We don't need to create and destroy the socket every time
    int fd;
    fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(IP_MULTICAST_PORT);

    // If we're not sending, get ready to receive
    if (!request.is_root) {
        bool yes = true;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

        addr.sin_addr.s_addr = htonl(INADDR_ANY);

        if (bind(fd, (struct sockaddr*) &addr, sizeof(addr)) < 0) {
            perror("bind");
            return 1;
        }
        // Request that the kernel join a multicast group
        struct ip_mreq mreq;
        mreq.imr_multiaddr.s_addr = inet_addr(IP_MULTICAST_ADDR);
        mreq.imr_interface.s_addr = htonl(INADDR_ANY);
        if (setsockopt(fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, (char*) &mreq, sizeof(mreq)) < 0) {
            perror("setsockopt");
            return 1;
        }
    }

    // This kills performance but makes sure that all receivers are ready before the sender starts
    ompi_coll_portals4_barrier_intra(comm, module);

    ssize_t nbytes;
    if (request.is_root) {
        addr.sin_addr.s_addr = inet_addr(IP_MULTICAST_ADDR);

        // First send the size so that the receivers know how many messages to expect
        // TODO: There are better ways to do this, no?

        nbytes = sendto(fd, &request.data_size, sizeof(size_t), 0, (struct sockaddr*) &addr, sizeof(addr));
        if (nbytes < 0)
            perror("sendto for size");
        // printf("Sent %zd for size\n", nbytes);
        char* send_next = request.data;
        size_t size_remaining = request.data_size;
        while (size_remaining > 0) {
            // TODO: UDP does not guarauntee ordering!!
            nbytes = sendto(fd, send_next, MIN(size_remaining, MAX_BCAST_SIZE), 0, (struct sockaddr*) &addr, sizeof(addr));
            if (nbytes < 0)
                perror("sendto");

            // printf("Sent %zd\n", nbytes);
            size_remaining -= nbytes;
            send_next += nbytes;
        }
    }
    else {
        int addrlen = sizeof(addr);

        // Get the size
        nbytes = recvfrom(fd, &request.data_size, sizeof(size_t), 0, (struct sockaddr *) &addr, &addrlen);
        if (nbytes < 0)
            perror("recvfrom for size");
        size_t size_remaining = request.data_size;
        // printf("Received %zd for size. Size is %zu\n", nbytes, size_remaining);
        char* recv_next = request.data;
        while (size_remaining > 0) {
            nbytes = recvfrom(fd, recv_next, MIN(size_remaining, MAX_BCAST_SIZE), 0, (struct sockaddr *) &addr, &addrlen);
            if (nbytes < 0)
                perror("recvfrom");
            recv_next += nbytes;
            size_remaining -= nbytes;
            // printf("Received %zd\n", nbytes);
        }
    }
    close(fd);

    post_bcast_data(&request);
    return (OMPI_SUCCESS);
}

END_C_DECLS