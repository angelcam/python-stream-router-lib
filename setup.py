# flake8: noqa

from setuptools import setup, find_packages

setup(
    name="streamrouter",
    version='1.2.5',
    description="Angelcam stream router library",
    keywords="asyncio stream router",
    author="Angelcam",
    author_email="dev@angelcam.com",
    url="https://bitbucket.org/angelcam/python-stream-router-lib/",
    license="MIT",
    packages=find_packages(),
    install_requires=[
        "murmurhash3 >= 2.3.5",
        "python-consul >= 0.6.1"
    ],
    dependency_links=[
        "https://bitbucket.org/angelcam/python-hmac-tokens/get/v1.1.2.tar.gz#egg=hmac_tokens"
    ],
    tests_require=[
        "pytest",
        "aiohttp"
    ],
    include_package_data=True,
    platforms='any',
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.5'
    ]
)
