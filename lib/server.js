'use strict';

var defaultConfig = require('./config');
var q = require('q');
var _ = require('lodash');

function initSync(mediator, mbaasApi, datasetId, syncOptions) {
  syncOptions = syncOptions || defaultConfig.syncOptions;

  var dataListHandler = function(datasetId, queryParams, metadata, cb) {
    var callback = getHandlerCallback(metadata, cb);
    mediator.request('wfm:cloud:' + datasetId + ':list', queryParams, {uid: null, timeout: 5000})
      .then(function(data) {
        var syncData = {};
        data.forEach(function(object) {
          syncData[object.id] = object;
        });
        callback(null, syncData);
      }, function(error) {
        console.log('Sync error: init:', datasetId, error);
        callback(error);
      });
  };

  var dataCreateHandler = function(datasetId, data, metadata, cb) {
    var callback = getHandlerCallback(metadata, cb);
    var ts = new Date().getTime();  // TODO: replace this with a proper uniqe (eg. a cuid)
    mediator.request('wfm:cloud:' + datasetId + ':create', [data, ts], {uid: ts})
      .then(function(object) {
        var res = {
          "uid": object.id,
          "data": object
        };
        callback(null, res);
      }, function(error) {
        console.log('Sync error: init:', datasetId, error);
        callback(error);
      });
  };

  var dataSaveHandler = function(datasetId, uid, data, metadata, cb) {
    var callback = getHandlerCallback(metadata, cb);
    mediator.request('wfm:cloud:' + datasetId + ':update', data, {uid: uid})
      .then(function(object) {
        callback(null, object);
      }, function(error) {
        console.log('Sync error: init:', datasetId, error);
        callback(error);
      });
  };

  var dataGetHandler = function(datasetId, uid, metadata, cb) {
    var callback = getHandlerCallback(metadata, cb);
    mediator.request('wfm:cloud:' + datasetId + ':read', uid)
      .then(function(object) {
        callback(null, object);
      }, function(error) {
        console.log('Sync error: init:', datasetId, error);
        callback(error);
      });
  };

  var dataDeleteHandler = function(datasetId, uid, metadata, cb) {
    var callback = getHandlerCallback(metadata, cb);
    mediator.request('wfm:cloud:' + datasetId + ':delete', uid)
      .then(function(message) {
        callback(null, message);
      }, function(error) {
        console.log('Sync error: init:', datasetId, error);
        callback(error);
      });
  };

  var collisionHandler = syncOptions.dataCollisionHandler;

  //start the sync service
  var deferred = q.defer();
  mbaasApi.sync.init(datasetId, syncOptions, function(err) {
    if (err) {
      console.log('Sync error: init:', datasetId, err);
      deferred.reject(err);
    } else {
      mbaasApi.sync.handleList(datasetId, dataListHandler);
      mbaasApi.sync.handleCreate(datasetId, dataCreateHandler);
      mbaasApi.sync.handleUpdate(datasetId, dataSaveHandler);
      mbaasApi.sync.handleRead(datasetId, dataGetHandler);
      mbaasApi.sync.handleDelete(datasetId, dataDeleteHandler);

      // set optional custom collision handler if its a function
      if (_.isFunction(collisionHandler)) {
        mbaasApi.sync.handleCollision(datasetId, collisionHandler);
      }
      deferred.resolve(datasetId);
    }
  });
  return deferred.promise;
}

function stop(mbaasApi, datasetId) {
  var deferred = q.defer();
  mbaasApi.sync.stop(datasetId, function() {
    deferred.resolve(datasetId);
  });
  return deferred.promise;
}

/**
 * Provides a backwards-compatibility check for fh-mbaas-api data handlers.
 * In versions <= 6.x.x data handlers accept a metadata object as the last argument
 *   and the callback function as the second last argument.
 * In versions 7.x.x data handlers accept a callback function as the last argument
 *   and the metadata object as the second last argument.
 * @param {Object|Function} cb1
 * @param {Object|Function} cb2
 * @returns {Function} The callback function for the data handler.
 */
function getHandlerCallback(cb1, cb2) {
  return typeof cb1 === 'function' ? cb1 : cb2;
}

module.exports = {
  init: initSync,
  stop: stop
};
