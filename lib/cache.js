
'use strict';

/*
 * This module handles all IO called on the cache (currently Redis)
 */

var url = require('url'),
    redis = require('redis'),
    lruCache = require('lru-cache');


function Cache(config, handlers) {
    if (!(this instanceof Cache)) {
        return new Cache(config, handlers);
    }

    var logHandler = handlers.logHandler || console.log,
        debugHandler = handlers.debugHandler || console.log;
    this.config = config;
    this.log = function (msg) {
        logHandler('Cache: ' + msg);
    };
    this.debug = function (msg) {
        debugHandler('Cache: ' + msg);
    };
    // Passive check enabled means: there is no active checks running
    this.passiveCheck = true;
    // Configure Redis
    this.redisClient = redis.createClient(
            this.config.redisPort,
            this.config.redisHost
            );
    this.redisClient.on('error', function (err) {
        this.log('RedisError ' + err);
    }.bind(this));
    this.redisPrefix = this.config.redisPrefix || '';
    // Monitor active checker every 30 seconds
    this.monitorActiveChecker();
    setInterval(this.monitorActiveChecker.bind(this), 30 * 1000);
    // LRU cache for Redis lookups
    if (this.config.server.lruCache === undefined) {
        this.debug('LRU cache is disabled');
        this.lru = {
            set: function () {},
            get: function () {},
            del: function () {}
        };
    } else {
        this.debug('LRU cache is enabled');
        this.lru = lruCache({
            max: this.config.server.lruCache.Size,
            maxAge: this.config.server.lruCache.ttl * 1000
        });
    }
}

/*
 * This method monitor if there is a active-health-checker. If there is one
 * running, it disables passive-health-checks and vice-versa.
 */
Cache.prototype.monitorActiveChecker = function () {
    this.redisClient.get('hchecker_ping', function (err, reply) {
        var newStatus = ((Math.floor(Date.now() / 1000) - reply) > 30);
        if (newStatus !== this.passiveCheck) {
            if (newStatus === false) {
                this.log("Disabling passive checks (active hchecker detected).");
            } else {
                this.log("Enabling passive checks (active hchecker stopped).");
            }
            this.passiveCheck = newStatus;
        }
    }.bind(this));
};

/*
 * This method mark a dead backend in the cache by its backend id
 */
Cache.prototype.markDeadBackend = function (backendInfo) {
    var frontendKey = backendInfo.frontend;
    if (backendInfo.frontendProto !== undefined) {
        frontendKey = backendInfo.frontendProto+':'+frontendKey;
    }
    var multi = this.redisClient.multi();
    // Update the Redis only if passive checks are enabled
    if (this.passiveCheck === true) {
        var redisKey = this.redisPrefix+'dead:'+frontendKey;
        multi.sadd(redisKey, backendInfo.backendId);
        multi.expire(redisKey, this.config.server.deadBackendTTL);
    }
    // Announce the dead backend on the "dead" channel
    multi.publish('dead', frontendKey + ';' +
            backendInfo.backendUrl + ';' + backendInfo.backendId + ';' +
            backendInfo.backendLen);
    multi.exec();
    // A dead backend invalidates the LRU
    this.lru.del(frontendKey);
};


/*
 * This method is an helper to get the domain name (without the subdomain)
 */
Cache.prototype.getDomainName = function (hostname) {
    var idx = hostname.lastIndexOf('.');

    if (idx < 0) {
        return hostname;
    }
    idx = hostname.lastIndexOf('.', idx - 1);
    if (idx < 0) {
        return hostname;
    }
    return hostname.substr(idx);
};


/*
 * This method picks up a backend randomly and ignore dead ones.
 * The parsed URL of the chosen backend is returned.
 * The method also decides which HTTP error code to return according to the
 * error.
 */
Cache.prototype.getBackend = function (host, callback) {
    var proto = undefined;
    if (typeof host === 'object') {
        if (!host.hasOwnProperty('host') || typeof host.host !== 'string') {
            return callback('invalid host', 400);
        }
        if (host.hasOwnProperty('proto') || typeof host.proto === 'string') {
            proto = host.proto;
        }
        host = host.host;
    } else if (typeof host !== 'string') {
        return callback('ivalid host', 400);
    }
    if (host === '__ping__') {
        return callback('ok', 200);
    }
    var index = host.indexOf(':');
    if (index > 0) {
        host = host.slice(0, index).toLowerCase();
    }

    var readFromCache = function (hostName, hostProto, cb) {
        var domainName = this.getDomainName(hostName);

        // keys are proto:name if proto is defined, otherwise just name 
        var hostKey = hostName;
        var domainKey = domainName = '*'+domainName;
        var wildcardKey = '*';
        if (hostProto !== undefined) {
            hostKey = hostProto+':'+hostKey;
            domainKey = hostProto+':'+domainKey;
            wildcardKey = hostProto+':'+wildcardKey;
        }

        // console.log({hostKey:hostKey, hostName:hostName, domainKey:domainKey, domainName:domainName, wildcardKey:wildcardKey});

        // Let's try the LRU cache first
        var rows = this.lru.get(hostKey);
        if (rows !== undefined) {
            return cb(rows);
        }
        var prefix = this.redisPrefix;
        // The entry is not in the LRU cache, let's do a request on Redis

        // Create a result list including backend search results and 
        // dead backend results
        var frontend_multi = this.redisClient.multi();

        // Search for a backend by querying different possible frontend keys:
        //  0) protocol-specific, subdomain-specific (https:my.domain.com)
        //  1) all-protocols,     subdomain-specific (my.domain.com)
        //  2) protocol-specific, all-subdomains     (https:*.domain.com)
        //  3) all-protocols,     all-subdomains     (*.domain.com)
        //  4) protocol-specific, any-domain         (https:*)
        //  5) all-protocols,     any-domain         (*)
        frontend_multi.lrange(prefix+'frontend:' + hostKey, 0, -1);
        frontend_multi.lrange(prefix+'frontend:' + hostName, 0, -1);        
        frontend_multi.lrange(prefix+'frontend:' + domainKey, 0, -1);
        frontend_multi.lrange(prefix+'frontend:' + domainName, 0, -1);
        frontend_multi.lrange(prefix+'frontend:' + wildcardKey, 0, -1);
        frontend_multi.lrange(prefix+'frontend:*', 0, -1);

        // Search for dead backends
        // 6) protocol-specific-rule dead backends
        // 7) all-protocols-rule dead backends
        frontend_multi.smembers(prefix+'dead:' + hostKey);
        frontend_multi.smembers(prefix+'dead:' + hostName);

        frontend_multi.exec(function (err, rows) {
            this.lru.set(hostKey, rows);
            cb(rows);
        }.bind(this));
    }.bind(this);

    readFromCache(host, proto, function (rows) {
        var backends, deads, frontendProto = undefined;
        // Get the relevant backend, even searches are protocol-specific
        for (var i=0; i<6; i++) {
            backends = rows[i];
            if (backends.length !== 0) {
                if (i%2 === 0) {
                    frontendProto = proto;
                    deads = rows[6];
                } else {
                    // matched all-protocols rule so leave frontendProto undefined
                    deads = rows[7];
                }
                break;
            }
        }
        if (backends.length === 0) {
            return callback('frontend not found', 400);
        }
        var virtualHost = backends[0];
        backends = backends.slice(1); // copy to not modify the lru in place
        var index = (function () {
            // Pickup a random backend index
            var indexes = [];
            for (var i = 0; i < backends.length; i += 1) {
                if (deads.indexOf(i.toString()) >= 0) {
                    continue; // Ignoring dead backends
                }
                indexes.push(i); // Keeping only the valid backend indexes
            }
            if (indexes.length < 2) {
                return (indexes.length === 1) ? indexes[0] : -1;
            }
            return indexes[Math.floor(Math.random() * indexes.length)];
        }());
        if (index < 0) {
            return callback('Cannot find a valid backend', 502);
        }
        console.log(backends[index])
        var backend = url.parse(backends[index]);
        backend.id = index; // Store the backend index
        backend.frontend = host; // Store the associated frontend
        backend.virtualHost = virtualHost; // Store the associated vhost
        backend.frontendProto = frontendProto; // the protocol of the request
        backend.len = backends.length;
        if (backend.hostname === undefined) {
            return callback('backend is invalid', 502);
        }
        backend.port = (backend.port === undefined) ? 80 : parseInt(backend.port, 10);
        this.debug('Proxying: ' + host + ' -> ' + backend.hostname + ':' + backend.port);
        callback(false, 0, backend);
    }.bind(this));
};

module.exports = Cache;
