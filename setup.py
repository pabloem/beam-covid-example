from distutils.core import setup

REQUIRED_PACKAGES = [
    'apache-beam[gcp,test]'
    # 'apache-beam[aws,gcp,test]'
]

setup(
  name='covidpipe',
  packages=['covidpipe'],
  long_description=open('README').read(),
  install_requires=REQUIRED_PACKAGES,
)
