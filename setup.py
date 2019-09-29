from setuptools import setup


with open('requirements.txt', 'r') as f:
    install_requires = f.read().split('\n')

__version__ = '0.0.0'
exec(open('avias_client/__init__.py').read())

setup(
    name='avias_client',
    version=__version__,
    packages=['avias_client'],
    url='',
    license='',
    author='mi',
    author_email='',
    description='',
    install_requires=install_requires
)
