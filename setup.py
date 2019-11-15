# flake8: noqa

# NOTE: this library requires libstreamrouter.so v0.8.x

from setuptools import setup, find_packages

setup(
    name="streamrouter",
    version='5.2.0',
    description="Angelcam stream router library",
    keywords="asyncio stream router",
    author="Angelcam",
    author_email="dev@angelcam.com",
    url="https://github.com/angelcam/python-stream-router-lib",
    license="MIT",
    packages=find_packages(),
    install_requires=[
    ],
    dependency_links=[
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
