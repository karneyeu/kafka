'use strict';

var net                    = require('net');
var Promise                = require('./bluebird-configured');
var NoKafkaConnectionError = require('./errors').NoKafkaConnectionError;
var tls                    = require('tls');
var _                      = require('lodash');

function Connection(options) {
    this.options = _.defaults(options || {}, {
        port: 9092,
        host: '127.0.0.1',
        connectionTimeout: 3000,
        initialBufferSize: 256 * 1024,
        ssl: {}
    });

    // internal state
    this.connected = false;
    this.closed = false; // raised if close() was called
    this.buffer = new Buffer(this.options.initialBufferSize);
    this.offset = 0;

    this.queue = {};
}

module.exports = Connection;

Connection.prototype.equal = function (host, port) {
    return this.options.host === host && this.options.port === port;
};

Connection.prototype.server = function () {
    return this.options.host + ':' + this.options.port;
};

Connection.prototype.connect = function (logger) {
    var self = this;

    if (self.connected) {
        if (logger) logger.debug('Connection:  connect->connected');
        return Promise.resolve();
    }

    if (self.connecting) {
        if (logger) logger.debug('Connection:  connect->connecting');
        return self.connecting;
    }
    if (logger) logger.debug('Connection:  connect->Promise.race');
    self.connecting = Promise.race([
        new Promise(function (resolve, reject) {
            setTimeout(function () {
                if (logger) logger.debug('Connection:  connect->timeout');
                reject(new NoKafkaConnectionError(self.server(), 'Connection timeout'));
            }, self.options.connectionTimeout);
        }),
        new Promise(function (resolve, reject) {
            var onConnect = function () {
                if (logger) logger.debug('Connection:  connect->onConnect');
                self.connected = true;
                resolve();
            };

            if (self.socket) {
                self.socket.destroy();
            }

            // If we have aborted/stopped during startup, don't progress.
            if (self.closed) {
                reject(new NoKafkaConnectionError(self.server(), 'Connection was aborted before connection was established.'));
                return;
            }

            if (self.options.ssl && self.options.ssl.cert && self.options.ssl.key) {
                self.socket = tls.connect(self.options.port, self.options.host, self.options.ssl, onConnect);
            } else {
                self.socket = net.connect(self.options.port, self.options.host, onConnect);
            }
            if (logger) logger.debug('Connection:  connect->connecting');

            self.socket.on('end', function () {
                if (logger) logger.debug('Connection:  connect->end');
                self._disconnect(new NoKafkaConnectionError(self.server(), 'Kafka server has closed connection'));
            });
            self.socket.on('error', function (err) {
                if (logger) logger.debug('Connection:  connect->error');
                var _err = new NoKafkaConnectionError(self.server(), err.toString());
                reject(_err);
                self._disconnect(_err);
            });
            self.socket.on('data', (d) => {
                self._receive(d, logger);
            });
        })
    ])
    .finally(function () {
        if (logger) logger.debug('Connection:  connect->connecting false');
        self.connecting = false;
    });
    if (logger) logger.debug('Connection:  connect->return');
    return self.connecting;
};

// Private disconnect method, this is what the 'end' and 'error'
// events call directly to make sure internal state is maintained
Connection.prototype._disconnect = function (err) {
    if (!this.connected) {
        return;
    }

    this.socket.end();
    this.connected = false;

    _.each(this.queue, function (t) {
        t.reject(err);
    });

    this.queue = {};
};

Connection.prototype._growBuffer = function (newLength) {
    var _b = new Buffer(newLength);
    this.buffer.copy(_b, 0, 0, this.offset);
    this.buffer = _b;
};

Connection.prototype.close = function () {
    var err = new NoKafkaConnectionError(this, 'Connection closed');
    err._kafka_connection_closed = true;
    this.closed = true;
    this._disconnect(err);
};

/**
 * Send a request to Kafka
 *
 * @param  {Buffer} data request message
 * @param  {Boolean} noresponse if the server wont send any response to this request
 * @return {Promise}      Promise resolved with a Kafka response message
 */
Connection.prototype.send = function (correlationId, data, noresponse, logger) {
    var self = this, buffer = new Buffer(4 + data.length);

    buffer.writeInt32BE(data.length, 0);
    data.copy(buffer, 4);

    function _send() {
        return new Promise(function (resolve, reject) {
            self.queue[correlationId] = {
                resolve: resolve,
                reject: reject
            };

            if (logger) logger.debug('Connection:  send->queue length:' + Object.keys(self.queue).length);
            if (logger) logger.debug('Connection:  send->write: ' + correlationId);
            self.socket.write(buffer);

            if (noresponse === true) {
                self.queue[correlationId].resolve();
                delete self.queue[correlationId];
            }
        });
    }

    if (!self.connected) {
        if (logger) logger.debug('Connection: connecting');
        return self.connect(logger).then(function () {
            if (logger) logger.debug('Connection:  connected->send');
            return _send();
        });
    }
    if (logger) logger.debug('Connection:  send');
    return _send();
};

Connection.prototype._receive = function (data, logger) {
    var length, correlationId;
    if (logger) logger.debug('Connection:  _receive');
    if (!this.connected) {
        if (logger) logger.debug('Connection:  _receive->not connected');
        return;
    }

    if (this.offset) {
        if (this.buffer.length < data.length + this.offset) {
            this._growBuffer(data.length + this.offset);
        }
        data.copy(this.buffer, this.offset);
        this.offset += data.length;
        data = this.buffer.slice(0, this.offset);
    }

    length = data.length < 4 ? 0 : data.readInt32BE(0);

    if (data.length < 4 + length) {
        if (this.offset === 0) {
            if (this.buffer.length < 4 + length) {
                this._growBuffer(4 + length);
            }
            data.copy(this.buffer);
            this.offset += data.length;
        }
        return;
    }

    this.offset = 0;

    correlationId = data.readInt32BE(4);
    if (logger) logger.debug('Connection:  _receive->correlationId: ' + correlationId);
    /*if (!this.queue.hasOwnProperty(correlationId)) {
        console.error('Wrong correlationId received:', correlationId);
    }*/

    this.queue[correlationId].resolve(new Buffer(data.slice(4, length + 4)));
    delete this.queue[correlationId];

    if (data.length > 4 + length) {
        this._receive(data.slice(length + 4), logger);
    }
};
