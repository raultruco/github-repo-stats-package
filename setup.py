from setuptools import find_packages, setup

setup(
    name='github-repo-stats',
    packages=find_packages(include=['githubrepostats']),
    version='0.1.6',
    description='Get github repository statistics',
    author='Me',
    license='MIT',
    install_requires=['numpy', 'pandas', 'requests'],
    setup_requires=['pytest-runner'],
    tests_require=['pytest==4.4.1'],
    test_suite='tests',
)
