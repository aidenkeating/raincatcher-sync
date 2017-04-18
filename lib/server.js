'use strict';

var defaultConfig = require('./config');
var q = require('q');
var _ = require('lodash');
var debug = require('./utils/logger')(__filename);

function initSync(mediator, mbaasApi, datasetId, syncOptions) {
  syncOptions = syncOptions || defaultConfig.syncOptions;
  debug('Sync init');
  var dataListHandler = function(datasetId, queryParams, metadata, cb) {
    mediator.request('wfm:cloud:' + datasetId + ':list', queryParams, {uid: null, timeout: 5000})
      .then(function(data) {
        var syncData = {};
        data.forEach(function(object) {
          object._id = getObjectId(object);
          syncData[object.id] = object;
        });
        cb(null, syncData);
      }, function(error) {
        debug('Sync error: init:', datasetId, error);
        cb(error);
      });
  };

  var dataCreateHandler = function(datasetId, data, metadata, cb) {
    var ts = new Date().getTime();  // TODO: replace this with a proper uniqe (eg. a cuid)
    mediator.request('wfm:cloud:' + datasetId + ':create', [data, ts], {uid: ts})
      .then(function(object) {
        var res = {
          "uid": object.id,
          "data": object
        };
        cb(null, res);
      }, function(error) {
        debug('Sync error: init:', datasetId, error);
        cb(error);
      });
  };

  var dataSaveHandler = function(datasetId, uid, data, metadata, cb) {
    mediator.request('wfm:cloud:' + datasetId + ':update', data, {uid: uid})
      .then(function(object) {
        cb(null, object);
      }, function(error) {
        debug('Sync error: init:', datasetId, error);
        cb(error);
      });
  };

  var dataGetHandler = function(datasetId, uid, metadata, cb) {
    mediator.request('wfm:cloud:' + datasetId + ':read', uid)
      .then(function(object) {
        object._id = getObjectId(object);
        cb(null, object);
      }, function(error) {
        debug('Sync error: init:', datasetId, error);
        cb(error);
      });
  };

  var dataDeleteHandler = function(datasetId, uid, metadata, cb) {
    mediator.request('wfm:cloud:' + datasetId + ':delete', uid)
      .then(function(message) {
        cb(null, message);
      }, function(error) {
        debug('Sync error: init:', datasetId, error);
        cb(error);
      });
  };

  var collisionHandler = syncOptions.dataCollisionHandler;

  //start the sync service
  var deferred = q.defer();
  mbaasApi.sync.init(datasetId, syncOptions, function(err) {
    if (err) {
      debug('Sync error: init:', datasetId, err);
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

      //Set optional custom hash function to deal with detecting model changes.
      if (_.isFunction(syncOptions.hashFunction)) {
        mbaasApi.sync.setRecordHashFn(datasetId, syncOptions.hashFunction);
      }

      deferred.resolve(datasetId);
    }
  });
  return deferred.promise;
}

/**
 * If the objects _id field is a Mongodb~ObjectID, convert it to a string.
 *
 * @param {String|Object} object - The object to check.
 * @returns {String} - The original _id field or the converted Mongodb~ObjectID
 */
function getObjectId(object) {
  if (object._id && typeof object._id === 'object') {
    return object._id.toHexString();
  }
  return object._id;
}

function stop(mbaasApi, datasetId) {
  var deferred = q.defer();
  mbaasApi.sync.stop(datasetId, function() {
    deferred.resolve(datasetId);
  });
  return deferred.promise;
}

module.exports = {
  init: initSync,
  stop: stop
};
