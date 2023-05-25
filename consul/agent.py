import requests
import logging
from requests import HTTPError
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from requests.adapters import (
    Retry,
    HTTPAdapter
)

logger = logging.getLogger(__name__)

_DEFAULT_STATUS = 'passing'
_DEFAULT_JITTER = 10

_API_AGENT_PATH = '/v1/agent'
_API_CATALOG_PATH = '/v1/catalog'


class ConsulAgent(object):

    def __init__(self,
                 consul_address: str,
                 service: str,
                 instance: str = None,
                 every_minutes: int = 1,
                 tls: bool = True,
                 token: str = None,
                 catalog_node: str = None,
                 message: dict = None):
        self._check_sub = ':heartbeat'

        self.tls = tls
        self.agent_uri = f"{'https://' if self.tls else 'http://'}{consul_address}{_API_AGENT_PATH}"
        self.catalog_uri = f"{'https://' if self.tls else 'http://'}{consul_address}{_API_CATALOG_PATH}"
        self.service = service
        self.instance = instance or service
        self.token = token
        self.every_minutes = every_minutes
        self.catalog_node = catalog_node
        self.message = message

        self._check_id = self.instance + self._check_sub

        self.session = None
        self.scheduler = None
        self.job = None

    def __enter__(self):
        logger.info("Activating client instance, job and scheduler")
        self._prepare()
        self.start(with_scheduler=True)
        logger.debug("Instance activated")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.info("Deactivating client instance, job and scheduler")
        self.stop()
        logger.debug("Instance deactivated")

    def _add_job(self):
        if self.message and isinstance(self.message, dict):
            self.job = self.scheduler.add_job(self.check_update_with_register,
                                              'cron',
                                              kwargs=self.message | {'check_id': self._check_id},
                                              minute=f'*/{self.every_minutes or 1}',
                                              jitter=_DEFAULT_JITTER)

    def start(self, with_scheduler=False):
        self._prepare()
        if with_scheduler:
            if not self.job:
                self._add_job()
            self.scheduler.start()

    def stop(self):
        if self.scheduler:
            if self.job:
                self.scheduler.remove_job(self.job.id)
                self.job = None
            self.scheduler.shutdown()
        if self.session:
            self.service_deregister(self.instance)
            if self.catalog_node:
                self.catalog_deregister(self.catalog_node)
            self.session.close()
            self.session = None

    def _prepare(self):
        if self.session is None:
            self.session = requests.Session()
            self.session.mount(prefix='https://' if self.tls else 'http://',
                               adapter=HTTPAdapter(
                                   max_retries=Retry(total=1,
                                                     backoff_factor=0.5,
                                                     status_forcelist=[500, 502, 503, 504])))
        if self.scheduler is None:
            self.scheduler = BackgroundScheduler(
                {
                    'apscheduler.executors.default': {
                        'class': 'apscheduler.executors.pool:ThreadPoolExecutor',
                        'max_workers': '2'
                    },
                    'apscheduler.executors.processpool': {
                        'type': 'processpool',
                        'max_workers': '2'
                    },
                    'apscheduler.job_defaults.coalesce': 'false',
                    'apscheduler.job_defaults.max_instances': '2',
                    'apscheduler.timezone': 'UTC'
                })

            def jobs_listener(event):
                log = logging.getLogger('apscheduler')
                log.setLevel(logging.INFO)

                if event.exception:
                    log.warning('job %s failed', event.job_id)
                else:
                    log.info('job %s executed successfully', event.job_id)

            self.scheduler.add_listener(jobs_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    def _reset_message(self, status=None, tentative=True):
        if self.job:
            self.scheduler.remove_job(self.job.id)
            self.service_deregister(self.instance)

        if not tentative:
            self.check_update_with_register(check_id=self._check_id,
                                            status=status or _DEFAULT_STATUS,
                                            **self.message)
        self._add_job()

    def _get(self, path, catalog=False):
        self._prepare()
        response = self.session.request(method='get',
                                        url=(self.agent_uri if not catalog else self.catalog_uri) + path,
                                        headers={'X-Consul-Token': self.token})
        response.raise_for_status()
        return response.json()

    def _put(self, path, request=None, params=None, catalog=False):
        self._prepare()
        _uri = (self.agent_uri if not catalog else self.catalog_uri) + path
        logger.debug(f"Calling PUT to {_uri} with params={params} and token {self.token}")
        logger.debug(f"JSON: {request}")
        response = self.session.request(method='put',
                                        url=_uri,
                                        json=request,
                                        params=params,
                                        headers={'X-Consul-Token': self.token})
        response.raise_for_status()
        return response.json() if response.content else None

    def set_heartbeat_message(self, address, port, force=True, status=_DEFAULT_STATUS):

        self._prepare()
        self.message = {
            'id': self.instance,
            'name': self.service,
            'address': address,
            'port': port,
            'checks': [{
                'CheckId': self._check_id,
                'TTL': f'{self.every_minutes * 60 + _DEFAULT_JITTER}s',
                'DeregisterCriticalServiceAfter': f'{self.every_minutes * 3}m',
            }]}
        self._reset_message(status=status, tentative=not force)
        return self

    def set_message(self, message, status=_DEFAULT_STATUS):
        self._prepare()
        self.message = message
        self._reset_message(status=status)
        return self

    def catalog_nodes(self):
        return self._get("/nodes", catalog=True)

    def catalog_services(self):
        return self._get("/services", catalog=True)

    def catalog_datacenters(self):
        return self._get("/datacenters", catalog=True)

    def catalog_deregister(self, node, entity=None, raw=None):
        request = {
            k: v
            for k, v in (
                ('ServiceID', entity or self.instance),
                ('Node', node or self.catalog_node)
            )
        }
        if raw is not None:
            request.update(raw)
        return self._put('/deregister', request=request, catalog=True)

    def service_list(self):
        return self._get("/services")

    def service_register(self, address, id, name=None, port=None, checks=None, raw=None):
        request = {
            k: v
            for k, v in (
                ('ID', id or self.instance),
                ('Name', name or self.service),
                ('Address', address),
                ('Port', port),
                ('Checks', checks),
            )
            if v is not None
        }
        if raw is not None:
            request.update(raw)
        return self._put('/service/register', request=request, params={'replace-existing-checks': 'true'})

    def service_details(self, service_id):
        return self._get("/service/" + service_id)

    def service_deregister(self, service_id):
        try:
            return self._put('/service/deregister/' + service_id)
        except HTTPError as e:
            if e.response.status_code == 404:
                logger.error(e)
                return None
            else:
                raise e

    def check_update(self, check_id, status='passing'):
        """update check status"""
        assert status in ('passing', 'warning', 'critical')
        return self._put('/check/update/' + check_id, {'Status': status})

    # agent level checks

    def check_list(self):
        return self._get('/checks')

    def check_register(self, check):
        return self._put('/check/register', check)

    def check_deregister(self, check_id):
        return self._put('/check/register/' + check_id)

    def check_update_with_register(self, check_id, status='passing', **register_kwargs):
        if register_kwargs is None:
            logger.debug("Dry run - message is not defined")
            return
        try:
            return self.check_update(check_id, status)
        except HTTPError as e:
            # catch only for content '''CheckID "backend_ttl_check" does not have associated TTL'''
            if e.response.status_code == 404 \
                    and f'"{check_id}"' in str(e.response.content):
                # service not registered, register
                self.service_register(**register_kwargs)
                return self.check_update(check_id, status)
            else:
                raise
