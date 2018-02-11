from cassandra.cqlengine import connection
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
import logging.config

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

##############################################################
# lab_cloud Models example:


class service_table(Model):
    # service info
    service_uuid = columns.UUID(primary_key=True)
    user = columns.Text(index=True)
    start_time = columns.BigInt(index=True)
    end_time = columns.BigInt(index=True)
    state = columns.Text(required=False)
    status = columns.Text(index=True)
    topofile = columns.Text(required=False)
    topology = columns.Text(required=False)
    reason = columns.Text(required=False)
    cnr_info = columns.Text(required=False)
    vnc_info = columns.Text(required=False)
    # cbr info
    cbr_name = columns.Text()
    cbr_num = columns.Integer()
    cbr_type = columns.Text()
    cbr_image = columns.Text(required=False)
    cbr_branch = columns.Text(required=False)
    cbr_label = columns.Text(required=False)
    # rpd info
    rpd_names = columns.List(value_type=columns.Text)
    rpd_num = columns.Integer()
    rpd_type = columns.Text()
    rpd_image = columns.Text(required=False)
    rpd_branch = columns.Text(required=False)
    rpd_label = columns.Text(required=False)
    # cm info
    cm_num = columns.Integer()
    # tng info
    tgn = columns.Text()
    tgn_port_speed = columns.Text(required=False)
    tgn_type = columns.Text(required=False)
    tgn_name = columns.Text(required=False)
    tgn_conn_ports = columns.List(value_type=columns.Text, required=False)


class user_rank_table(Model):
    user = columns.Text(primary_key=True)
    time = columns.BigInt()
    rank = columns.Integer(index=True)


class cbr_rank_table(Model):
    cbr_type = columns.Text(primary_key=True)
    count = columns.Integer()
    rank = columns.Integer()
############################################################

###########################################################


class CassOperation(object):
    '''
    Class to establish cassandra connection
    How to Use:
    step 1: define the cluster_list and the keyspace
        CLUSTER_LIST = ['cassandra']
        KEYSPACE = 'lab_cloud'
    step2: instantiate the class to use the opration function
        cass = CassOperation()
        '''

    def __init__(self, cluster_list, keyspace):
        try:
            connection.setup(cluster_list, keyspace, protocol_version=3)
            self.cluster = connection.cluster
            self.session = connection.session
            logger.info("Connect to cassandra..%s. Success" % keyspace)
        except Exception as e:
            logger.error(e)

    def __del__(self):
        self.session.shutdown()
        self.cluster.shutdown()
        logger.info("Disconnected with cassandra")

    def operation_fun(self):
        # You can add the cassandra operation code here you need
        pass
