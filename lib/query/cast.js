/**
 * Module dependencies
 */
var _         = require('lodash')
  , autocast  = require('autocast')
  , utils     = require('../utils')
  , ObjectId  = require('mongodb').ObjectID
;


function Cast() {
  return this;
}


Cast.prototype.cast = function (val, type) {
  var self = this;

  if (!type) {
    return autocast(val);
  }

  if (!_.isFunction(self[type])) {
    return val;
  }

  if (_.isPlainObject(val)) {
    return val;
  }

  if (_.isArray(val)) {
    return _.map(val, function (val) {
      return self.cast(val, type);
    });
  }

  return self[type](val);
};


Cast.prototype.integer  =
Cast.prototype.float    = function (val) {
  var number = +val;
  return _.isNaN(number) ? val : number;
};


Cast.prototype.date     =
Cast.prototype.time     =
Cast.prototype.datetime = function (val) {
  if (Object.prototype.toString.call(val) === '[object Date]') return val;
  var time = /^\d+$/.exec(val) ? +val : Date.parse(val);
  return _.isNaN(time) ? val : new Date(time);
};


Cast.prototype.boolean = function (val) {
  if (val === '1' || val === 'true' ) return true;
  if (val === '0' || val === 'false') return false;
  return Boolean(val);
};


Cast.prototype.array  =
Cast.prototype.binary =
Cast.prototype.json   = function (val) {
  return val;
};


Cast.prototype.text       =
Cast.prototype.mediumtext =
Cast.prototype.longtext   = function (val) {
  // return _.isString(val) ? val : _.isFunction(val.toString) ? val.toString() : '' + val;
  // String, RegExp
  return val;
};


Cast.prototype.string = function (val) {
  // String, RegExp
  return val;
};


// Id attribute and all association attributes are objectid type by default
// (@see { @link collection._parseDefinition }).
Cast.prototype.objectid = function (val) {
  // Check for Mongo ObjectId
  if (_.isString(val) && utils.matchMongoId(val)) {
    return new ObjectId(val.toString());
  }

  return val;
};


module.exports = _.bind(Cast.prototype.cast, new Cast());