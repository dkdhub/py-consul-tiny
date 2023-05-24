from setuptools import setup

setup(name='py-consul-tiny',
      version='0.1',
      description='The tiny Consul client for python 3.x',
      url='https://github.com/dkdhub/py-consul-tiny',
      author='UPDCon',
      author_email='support@updcon.net',
      license='MIT',
      packages=['consul'],
      install_requires=['requests', 'apscheduler'])
