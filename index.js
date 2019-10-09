var _ = require('underscore'),
    // console = require('../console.js'),
    // $fh = require('fh-mbaas-api'),
    mongoClient = require('alt-fh-db').client,
    async = require('async');
    // ERRORS = require('../errorCodes').dbErrors;
// DITCH_REQ_LIMIT = require('../constants').DITCH_REQ_LIMIT;

function noop() {}


// function taskRunner(func, qCallback) {
//     func.call(undefined, qCallback);
// }

// var queues = {
//     'CREATE': async.queue(taskRunner, DITCH_REQ_LIMIT),
//     'READ': async.queue(taskRunner, DITCH_REQ_LIMIT),
//     'UPDATE': async.queue(taskRunner, DITCH_REQ_LIMIT),
//     'LIST': async.queue(taskRunner, DITCH_REQ_LIMIT),
//     'DELETE': async.queue(taskRunner, DITCH_REQ_LIMIT),
//     'DELETEALL': async.queue(taskRunner, DITCH_REQ_LIMIT)
// }

// Generic wrapper for all database callbacks
// Ensures logging occurs and a client friendly error is propogated up
function dbCb(cb) {
    return function (err, res) {
        if (err) {
            // console.error(ERRORS.DB_ERROR.DEV_MSG, err);
            // return cb(ERRORS.DB_ERROR.CLIENT_MSG, null);
        }
        return cb(null, res);
    };
}

exports.create = function (col, fields, cb) {
    console.log('[agg-mongo-dal/index.js:37] In create');
    console.log('col:',col,'fields:',fields);
    // Note create time of items
    if (_.isArray(fields))  {
        fields.forEach( function (field) {
            field._createDateTime = Date.now();
            field._createDateTimeHuman = new Date();
        });
    } else {
        fields._createDateTime = Date.now();
        fields._createDateTimeHuman = new Date();
    }

    mongoClient.db({
        'act': 'create',
        'type': col,
        'fields': fields
    }, dbCb(cb));

    // var func = function (callback) {
    //     mongoClient.db({
    //         'act': 'create',
    //         'type': col,
    //         'fields': fields
    //     }, dbCb(callback));
    // }
    // // Queue //TODO:Brid - replicate for all above
    // queues['CREATE'].push(func, cb);
};

exports.read = function (col, guid, cb) {
    mongoClient.db({
        'act': 'read',
        'type': col,
        'guid': guid
    }, dbCb(cb));
};

exports.update = function (col, guid, fields, cb) {
    // Track when items are updated
    fields._lastModified = Date.now();

    mongoClient.db({
        'act': 'update',
        'type': col,
        'fields': fields,
        'guid': guid
    }, dbCb(cb));
};

exports.list = function (col, restrictions, cb) {
    var params = {
        'act': 'list',
        'type': col
    };

    if (restrictions && typeof restrictions === 'function') {
        cb = restrictions;
        restrictions = null;
    }
    else if (restrictions) {
        params = _.extend(params, restrictions);
    }

    mongoClient.db(params, dbCb(cb));
};

exports.listWithFields = function (col, restrictions, fields, cb) {
    var params = {
        'act': 'list',
        'type': col,
        'fields': fields
    };
    if (restrictions) {
        params = _.extend(params, restrictions);
    }
    mongoClient.db(params, dbCb(cb));
};

var remove = exports.remove = function (col, guid, cb) {
    mongoClient.db({
        'act': 'delete',
        'type': col,
        guid: guid
    }, dbCb(cb));
};


var removeAll = exports.removeAll = function (col, cb) {
    console.log('Deleting collection: %s', col);
    mongoClient.db({
        'act': 'deleteall',
        'type': col
    }, dbCb(cb));
};


exports.removeAllCollections = function (collections, callback) {

    // callback = callback || noop;

    async.eachSeries(collections, function (col, cb) {
        removeAll(col, cb);
    }, function (err) {
        callback(err);
    });
};


/**
 * Remove collections based on GUIDs
 */
exports.removeByIds = function (col, ids, cb) {
    var funcs = [];
    cb = cb || noop;

    _.each(ids, function (id) {
        funcs.push(function (callback) {
            remove(col, id, callback);
        });
    });

    async.parallel(funcs, function (err, data) {
        cb(err, data);
    });
};


exports.genericQuery = function (query, cb) {
    mongoClient.db(query, cb);
};
