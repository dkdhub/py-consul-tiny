import requests
import logging
from requests import HTTPError
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from requests.adapters import (
    Retry,
    HTTPAdapter
)


class ConsulAgent(object):

    def __init__(self, path, service, token=None):
        self.path = path
        self.service = service
        self.token = token
        self.scheduler = BackgroundScheduler(
            {
                'apscheduler.executors.default': {
                    'class': 'apscheduler.executors.pool:ThreadPoolExecutor',
                    'max_workers': '20'
                },
                'apscheduler.executors.processpool': {
                    'type': 'processpool',
                    'max_workers': '2'
                },
                'apscheduler.job_defaults.coalesce': 'false',
                'apscheduler.job_defaults.max_instances': '2',
                'apscheduler.timezone': 'UTC'
            })
        self.session = requests.Session()
        self.session.mount(adapter=HTTPAdapter(
            max_retries=Retry(total=1,
                              backoff_factor=0.5,
                              status_forcelist=[500, 502, 503, 504])))

        def jobs_listener(event):
            log = logging.getLogger('apscheduler')
            log.setLevel(logging.INFO)

            if event.exception:
                log.warning('job %s failed', event.job_id)
            else:
                log.info('job %s executed successfully', event.job_id)

        self.scheduler.add_listener(jobs_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        self.job = self.scheduler.add_job(self.check_update_with_register,
                                          'cron',
                                          kwargs={
                                              'check_id': self.service + ':ttl_check',
                                              'id': self.service,
                                              'address': '127.0.0.1',
                                              'port': 8080,
                                              'checks': [{
                                                  'CheckId': self.service + ':ttl_check',
                                                  'TTL': '40s',
                                                  'DeregisterCriticalServiceAfter': '15m',
                                              }]},
                                          hour='*/1')

    def __del__(self):
        self.scheduler.remove_job(self.job.id)
        self.scheduler.shutdown()
        self.session.close()
        self.service_deregister(self.service)

    def _get(self, path):
        response = self.session.get(self.path + path)
        response.raise_for_status()
        return response.json()

    def _put(self, path, request=None):
        response = self.session.put(self.path + path, json=request)
        response.raise_for_status()
        return response.json() if response.content else None

    def service_list(self):
        return self._get("/services")

    def service_register(self, address, id, name=None, port=None, checks=None, raw=None):
        request = {
            k: v
            for k, v in (
                ('ID', id),
                ('Name', name or id),
                ('Address', address),
                ('Port', port),
                ('Checks', checks),
            )
            if v is not None
        }
        if raw is not None:
            request.update(raw)
        return self._put('/service/register', request)

    def service_details(self, service_id):
        return self._get("/service/" + service_id)

    def service_deregister(self, service_id):
        return self._put('/service/deregister/' + service_id)

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
        try:
            return self.check_update(check_id, status)
        except HTTPError as e:
            # catch only for content '''CheckID "backend_ttl_check" does not have associated TTL'''
            if e.response.status_code >= 400 and '"%s"' % (
                    check_id,) in e.response.content and 'TTL' in e.response.content:
                # service not registered, register
                self.service_register(**register_kwargs)
                return self.check_update(check_id, status)
            else:
                raise
