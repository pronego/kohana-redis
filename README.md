Kohana Redis Module
===================

- redis cache driver with tagging support
- redis session support
- credis client wrapper with integrated lua script execution support 

Requirements
------------

The session handler requires redis version 2.0. The session driver relies on lua scripting, therefore version 2.8 is required. Phpredis module is optional (and recommended), however plain php sockets are sufficient.

Installation
------------

1. git submodule add https://github.com/mutant-industries/kohana-redis.git modules/redis
2. cd modules/redis
3. composer update
4. enable module in your bootstrap.php file
5. customize config files

Usage
-----

The usage of cache and session drivers is no different to usage of any other kohana cache / session implementation. All options are well documented in default configuration files, which is probably the best starting point to using this module.
There is also configuration for default redis connection. Any number of independent connections can be configured, the 'connection' parameter in session / cache config determines the config name, that will be used.
The implementation of cache tagging is not compatible with redis-cluster, for high availability caching solution please consider some master-slave setup (e.g. sentinel).
The Redis_Client, which is an extension to Credis_Client by Colin Mollenhour with improved lua scripting support, that also fits Kohana framework, can be used directly. Getting the client is as simple as

    Redis_Client::instance('config_group')
