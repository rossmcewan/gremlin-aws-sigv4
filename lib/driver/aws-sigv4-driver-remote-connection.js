/* eslint no-underscore-dangle: ["error", { "allowAfterThis": true }] */

const debug = require('debug')('gremlin-aws-sigv4:driver');
const EventEmitter = require('events');
const gremlin = require('gremlin');
const util = require('util');
const WebSocket = require('ws');
const { uuid, getUrlAndHeaders, bufferFromString } = require('../utils');

const responseStatusCode = {
  success: 200,
  noContent: 204,
  partialContent: 206,
};

const pingIntervalDelay = 5 * 1000;
const pongTimeoutDelay = 2 * 1000;

class AwsSigV4DriverRemoteConnection extends EventEmitter {
  constructor(host, port, options = {}) {
    super();

    this.host = host;
    this.port = port;
    this.options = options;

    this._responseHandlers = {};
    this._reader = options.reader || new gremlin.structure.io.GraphSONReader();
    this._writer = options.writer || new gremlin.structure.io.GraphSONWriter();
    this._openPromise = null;
    this._openCallback = null;
    this._closePromise = null;
    this._closeCallback = null;
    this._pingInterval = null;
    this._pongTimeout = null;

    /**
     * Gets the MIME type.
     * @type {String}
     */
    this.mimeType = options.mimeType || 'application/vnd.gremlin-v2.0+json';

    this._header = String.fromCharCode(this.mimeType.length) + this.mimeType;
    this.isOpen = false;
    this.traversalSource = options.traversalSource || 'g';

    this._pingEnabled = this.options.pingEnabled === false ? false : true;
    this._pingIntervalDelay = this.options.pingInterval || pingIntervalDelay;
    this._pongTimeoutDelay = this.options.pongTimeout || pongTimeoutDelay;

    this.autoReconnect = options.autoReconnect || false;
    this.maxRetry = options.maxRetry || 10;
    this.try = 0;

    if (this.options.connectOnStartup !== false) {
      this.open();
    }
  }

  /**
   * Opens the connection, if its not already opened.
   * @returns {Promise}
   */
  open() {
    if (this.isOpen) {
      return Promise.resolve();
    }
    if (this._openPromise) {
      return this._openPromise;
    }

    this.emit('log', `ws open`);

    const { url, headers } = getUrlAndHeaders(this.host, this.port, this.options);
    this._ws = new WebSocket(url, { headers });
    this._ws.on('message', (data) => this._handleMessage(data));
    this._ws.on('error', (err) => this._handleError(err));
    this._ws.on('close', (code, message) => this._handleClose(code, message));

    this._ws.on('pong', () => {
      this.emit('log', 'ws pong received');
      if (this._pongTimeout) {
        clearTimeout(this._pongTimeout);
        this._pongTimeout = null;
      }
    });
    this._ws.on('ping', () => {
      this.emit('log', 'ws ping received');
      this._ws.pong();
    });

    return this._openPromise = new Promise((resolve, reject) => {
      this._ws.on('open', () => {
        this.isOpen = true;
        this.try = 0;
        if (this._pingEnabled) {
          this._pingHeartbeat();
        }
        resolve();
      });
    });
  }

  /**
   * Deactivates auto-reconnection and closes the connection if not already closed.
   * @return {Promise}
   */
  close() {
    this.autoReconnect = false;
    if (this.isOpen === false) {
      return Promise.resolve();
    }
    if (!this._closePromise) {
      this._closePromise = new Promise(resolve => {
        this._closeCallback = resolve;
        this._ws.close();
      });
    }
    return this._closePromise;
  }

  submit(bytecode) {
    return this.open()
      .then(() => new Promise((resolve, reject) => {
        const requestId = uuid();
        this._responseHandlers[requestId] = {
          callback: (err, result) => (err ? reject(err) : resolve(result)),
          result: null,
        };
        const message = Buffer.from(this._header
          + JSON.stringify(this._getRequest(requestId, bytecode)));
        this._ws.send(message);
      }));
  }

  _getRequest(id, bytecode) {
    console.log(this._writer);
    console.log('---------------')
    return ({
      requestId: { '@type': 'g:UUID', '@value': id },
      op: 'bytecode',
      processor: 'traversal',
      args: {
        gremlin: this._writer.adaptObject(bytecode),
        aliases: { g: this.traversalSource },
      },
    });
  }

  _handleError(error) {
    this.emit('log', `ws error ${err}`);
    this._cleanupWebsocket();
    this.emit('error', err);
  }

  _handleClose(code, message) {
    this.emit('log', `ws close code=${code} message=${message}`);
    this._cleanupWebsocket();
    if (this._closeCallback) {
      this._closeCallback();
    }
    this.emit('close', code, message);
  }

  _handleMessage(data) {
    const response = this._reader.read(JSON.parse(data.toString()));
    if (response.requestId === null || response.requestId === undefined) {
      // There was a serialization issue on the server that prevented the parsing of the request id
      // We invoke any of the pending handlers with an error
      Object.keys(this._responseHandlers).forEach((requestId) => {
        const handler = this._responseHandlers[requestId];
        this._clearHandler(requestId);
        if (response.status !== undefined && response.status.message) {
          return handler.callback(
            new Error(util.format(
              'Server error (no request information): %s (%d)', response.status.message, response.status.code,
            )),
          );
        }
        return handler.callback(new Error(util.format('Server error (no request information): %j', response)));
      });
      return undefined;
    }

    const handler = this._responseHandlers[response.requestId];

    if (!handler) {
      return undefined;
    }

    if (response.status.code >= 400) {
      // callback in error
      return handler.callback(
        new Error(util.format('Server error: %s (%d)', response.status.message, response.status.code)),
      );
    }
    switch (response.status.code) {
      case responseStatusCode.noContent:
        this._clearHandler(response.requestId);
        return handler.callback(null, { traversers: [] });
      case responseStatusCode.partialContent:
        handler.result = handler.result || [];
        handler.result.push(...response.result.data);
        break;
      default:
        if (handler.result) {
          handler.result.push(...response.result.data);
        } else {
          handler.result = response.result.data;
        }
        this._clearHandler(response.requestId);
        return handler.callback(null, { traversers: handler.result });
    }
    return undefined;
  }

  /**
   * Clears the internal state containing the callback and result buffer of a given request.
   * @param requestId
   * @private
   */
  _clearHandler(requestId) {
    delete this._responseHandlers[requestId];
  }

  _pingHeartbeat() {
    if (this._pingInterval) {
      clearInterval(this._pingInterval);
      this._pingInterval = null;
    }

    this._pingInterval = setInterval(() => {
      if (this.isOpen === false) {
        // in case of if not open..
        if (this._pingInterval) {
          clearInterval(this._pingInterval);
          this._pingInterval = null;
        }
      }

      this._pongTimeout = setTimeout(() => {
        this._ws.terminate();
      }, this._pongTimeoutDelay);

      this._ws.ping();

    }, this._pingIntervalDelay);
  }

  /**
   * clean websocket context
   */
  _cleanupWebsocket() {
    if (this._pingInterval) {
      clearInterval(this._pingInterval);
    }
    this._pingInterval = null;
    if (this._pongTimeout) {
      clearTimeout(this._pongTimeout);
    }
    this._pongTimeout = null;

    this._ws.removeAllListeners();
    this._openPromise = null;
    this._closePromise = null;
    this.isOpen = false;
  }
}

module.exports = AwsSigV4DriverRemoteConnection;
