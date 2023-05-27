import logging
import time
from consul.agent import ConsulAgent

logging.basicConfig(format="%(levelname)s %(asctime)s - %(name)s -  %(message)s",
                    level=logging.DEBUG)

agent = ConsulAgent(consul_address="127.0.0.1:8500",
                    service='agent-test-service',
                    tls=False,
                    catalog_node='consul')

logger = logging.getLogger(__name__)

logger.info(f"== Agent instance: {agent.__dict__}")

logger.info("== Checking start/stop")
agent.start()
agent.stop()

logger.info("== Checking context manager")
with agent:
    time.sleep(10)

logger.info("== Checking heartbeat")
logger.info("-- set HB message (no force)")
agent.set_heartbeat_message("127.1.1.1", 9876)
logger.info("-- set HB message (force registration)")
agent.set_heartbeat_message("127.000.000.111", 10000, force=True)
time.sleep(10)
logger.info("-- service agent-test-service should be OK (green)")
time.sleep(60)
logger.info("-- service agent-test-service should be FAILED (red)")
logger.info("-- context manager should autostart the HB messaging")
with agent as a:
    time.sleep(60)
    logger.info("-- time gone - stopping HB and remove entity")

logger.info("== avoid entity auto removal")
agent.autoremove_catalog_record(False)
with agent as a:
    time.sleep(90)
    logger.info(a.catalog_services())
logger.info(agent.catalog_services())

logger.info("== Pouring back")
agent.autoremove_catalog_record(True)
agent.start()
logger.info(agent.catalog_services())
agent.stop()
logger.info(agent.catalog_services())

logger.info("== DONE")
