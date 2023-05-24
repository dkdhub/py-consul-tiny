import requests
from requests import HTTPError


class ConsulAgent(object):

    def __init__(self, path, token=None):
        self.path = path
        self.token = token

    def _get(self, path):
        response = requests.get(self.path + path)
        response.raise_for_status()
        return response.json()

    def _put(self, path, request=None):
        response = requests.put(self.path + path, json=request)
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
