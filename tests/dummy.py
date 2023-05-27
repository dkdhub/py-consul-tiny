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



