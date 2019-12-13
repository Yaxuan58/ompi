
#ifndef MCA_COLL_IPMULTICAST_EXPORT_H
#define MCA_COLL_IPMULTICAST_EXPORT_H

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

#include "ompi/mca/coll/portals4/coll_portals4.h"

BEGIN_C_DECLS


int ompi_coll_ipmulticast_bcast(void *buff, int count,
        struct ompi_datatype_t *datatype, int root,
        struct ompi_communicator_t *comm,mca_coll_base_module_t *module);

END_C_DECLS
#endif
