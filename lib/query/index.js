
/**
 * Module dependencies
 */

var _ = require('lodash'),
ObjectId = require('mongodb').ObjectID,
Aggregate = require('./aggregate'),
utils = require('../utils'),
cast = require('./cast'),
hop = utils.object.hasOwnProperty;

/**
 * Query Constructor
 *
 * Normalizes Waterline queries to work with Mongo.
 *
 * @param {Object} options
 * @api private
 */

var Query = module.exports = function Query(options, schema) {

  // Flag as an aggregate query or not
  this.aggregate = false;

  // Cache the schema for use in parseTypes
  this.schema = schema;

  // Check for Aggregate Options
  this.checkAggregate(options);

  // Normalize Criteria
  this.criteria = this.normalizeCriteria(options);

  return this;
};

/**
 * Check For Aggregates
 *
 * Checks the options to determine if an aggregate query is needed.
 *
 * @param {Object} options
 * @api private
 */

Query.prototype.checkAggregate = function checkAggregate(options) {
  var aggregateOptions = ['groupBy', 'sum', 'average', 'min', 'max'];
  var aggregates = _.intersection(aggregateOptions, Object.keys(options));

  if(aggregates.length === 0) return options;

  this.aggregateGroup = new Aggregate(options);
  this.aggregate = true;
};


/**
 * Normalize Criteria
 *
 * Transforms a Waterline Query into a query that can be used
 * with MongoDB. For example it sets '>' to $gt, etc.
 *
 * @param {Object} options
 * @return {Object}
 * @api private
 */

Query.prototype.normalizeCriteria = function normalizeCriteria(options) {
  "use strict";
  var self = this;

  return _.reduce(options, function (criteria, original, key) {
    switch (key) {
      case 'where':
        criteria.where  = self.parseWhere(original);
        break;
      case 'sort':
        criteria.sort   = self.parseSort(original);
        break;
      case 'select':
        criteria.fields = _.extend(criteria.fields || {}, _(original).map(function (item) { return [item, true] }).object().value());
        break;
      case 'prune':
        criteria.fields = _.extend(criteria.fields || {}, _(original).map(function (item) { return [item, false] }).object().value());
        break;
      case 'options':
        _.extend(criteria, original);
        break;
      default:
        criteria[key] = original;
    }
    return criteria;
  }, {});
};


/**
 * Parse Where
 *
 * <where> ::= <clause>
 *
 * @api private
 *
 * @param original
 * @returns {*}
 */
Query.prototype.parseWhere = function parseWhere(original) {
  "use strict";
  var self = this;

  // Fix an issue with broken queries when where is null
  if(_.isNull(original)) return {};

  return self.parseClause(original);
};


/**
 * Parse Clause
 *
 * <clause> ::= { <clause-pair>, ... }
 *
 * <clause-pair> ::= <field> : <expression>
 *                 | or|$or: [<clause>, ...]
 *                 | $or   : [<clause>, ...]
 *                 | $and  : [<clause>, ...]
 *                 | $nor  : [<clause>, ...]
 *                 | like  : { <field>: <expression>, ... }
 *
 * @api private
 *
 * @param original
 * @returns {*}
 */
Query.prototype.parseClause = function parseClause(original) {
  "use strict";
  var self = this;

  return _.reduce(original, function parseClausePair(obj, val, key) {
    "use strict";

    // Normalize `or` key into mongo $or
    if (key.toLowerCase() === 'or') key = '$or';

    // handle Logical Operators
    if (['$or', '$and', '$nor'].indexOf(key) !== -1) {
      // Value of $or, $and, $nor require an array, else ignore
      if (_.isArray(val)) {
        val = _.map(val, function (clause) {
          return self.parseClause(clause);
        });

        obj[key] = val;
      }
    }

    // handle Like Operators for WQL (Waterline Query Language)
    else if (key.toLowerCase() === 'like') {
      // transform `like` clause into multiple `like` operator expressions
      _.extend(obj, _.reduce(val, function parseLikeClauses(likes, expression, field) {
        likes[field] = self.parseExpression(field, { like: expression });
        return likes;
      }, {}));
    }

    // Default
    else {
      val = self.parseExpression(key, val);

      // Normalize `id` key into mongo `_id`
      if (key === 'id' && !hop(this, '_id')) key = '_id';

      // Check if the attribute is embed, and if so then turn it into sub-document query
      if(hop(self.schema, key) && self.schema[key]['embed']) {
        key = key + '._id';
      }

      obj[key] = val;
    }

    return obj;
  }, {}, original);
};


/**
 * Parse Expression
 *
 * <expression> ::= { <!|not>: <value> | [<value>, ...] }
 *                | { <$not>: <expression>, ... }
 *                | { <modifier>: <value>, ... }
 *                | [<value>, ...]
 *                | <value>

 * @api private
 *
 * @param field
 * @param expression
 * @returns {*}
 */
Query.prototype.parseExpression = function parseExpression(field, expression) {
  "use strict";
  var self = this;

  // Assume that expression that included primaryKey is representing a document
  // in the case of denormalization. Replace with primaryKey itself. When we use
  // denormalization and populate models associated, adapter.join() will call find()
  // with a sub-document as primaryKey.
  if (_.isPlainObject(expression) && hop(expression, field)
    && hop(self.schema, field) && self.schema[field].primaryKey) {
    expression = expression[field];
  }

  // Recursively parse nested unless value is a date
  if (_.isPlainObject(expression) && !_.isDate(expression)) {
    return _.reduce(expression, function (obj, val, modifier) {

      // Handle `not` by transforming to $not, $ne or $nin
      if (modifier === '!' || modifier.toLowerCase() === 'not') {

        if (_.isPlainObject(val) && !_.has(val, '_bsontype')) {
          obj['$not'] = self.parseExpression(field, val);
          return obj;
        }

        val = self.parseValue(field, modifier, val);
        modifier = _.isArray(val) ? '$nin' : '$ne';
        obj[modifier] = val;
        return obj;
      }

      // WQL Evaluation Modifiers for String
      if (_.isString(val)) {
        // Handle `contains` by building up a case insensitive regex
        if(modifier === 'contains') {
          val = utils.caseInsensitive(val);
          val =  '.*' + val + '.*';
          obj['$regex'] = new RegExp('^' + val + '$', 'i');
          return obj;
        }

        // Handle `like`
        if(modifier === 'like') {
          val = utils.caseInsensitive(val);
          val = val.replace(/%/g, '.*');
          obj['$regex'] = new RegExp('^' + val + '$', 'i');
          return obj;
        }

        // Handle `startsWith` by setting a case-insensitive regex
        if(modifier === 'startsWith') {
          val = utils.caseInsensitive(val);
          val =  val + '.*';
          obj['$regex'] = new RegExp('^' + val + '$', 'i');
          return obj;
        }

        // Handle `endsWith` by setting a case-insensitive regex
        if(modifier === 'endsWith') {
          val = utils.caseInsensitive(val);
          val =  '.*' + val;
          obj['$regex'] = new RegExp('^' + val + '$', 'i');
          return obj;
        }
      }

      // Handle `lessThan` by transforming to $lt
      if(modifier === '<' || modifier === 'lessThan' || modifier.toLowerCase() === 'lt') {
        obj['$lt'] = self.parseValue(field, modifier, val);
        return obj;
      }

      // Handle `lessThanOrEqual` by transforming to $lte
      if(modifier === '<=' || modifier === 'lessThanOrEqual' || modifier.toLowerCase() === 'lte') {
        obj['$lte'] = self.parseValue(field, modifier, val);
        return obj;
      }

      // Handle `greaterThan` by transforming to $gt
      if(modifier === '>' || modifier === 'greaterThan' || modifier.toLowerCase() === 'gt') {
        obj['$gt'] = self.parseValue(field, modifier, val);
        return obj;
      }

      // Handle `greaterThanOrEqual` by transforming to $gte
      if(modifier === '>=' || modifier === 'greaterThanOrEqual' || modifier.toLowerCase() === 'gte') {
        obj['$gte'] = self.parseValue(field, modifier, val);
        return obj;
      }

      obj[modifier] = self.parseValue(field, modifier, val);
      return obj;
    }, {});
  }

  // <expression> ::= [value, ...], normalize array into mongo $in operator expression
  if (_.isArray(expression)) {
    return { $in: self.parseValue(field, '$in', expression) };
  }

  // <expression> ::= <value>, default equal expression
  return self.parseValue(field, undefined, expression);
};


/**
 * Parse Value
 *
 * <value> ::= RegExp | Number | String
 *           | [<value>, ...]
 *           | <plain object>
 *
 * @api private
 *
 * @param field
 * @param modifier
 * @param val
 * @returns {*}
 */
Query.prototype.parseValue = function parseValue(field, modifier, val) {
  "use strict";
  var self = this;

  if (val === 'null') return null;

  // Look and see if the key is in the schema, if NOT, don't change the value.
  if (!hop(self.schema, field)) {
    return cast(val);
  }

  // Lookup the type of current field.
  var type = self.schema[field].type || self.schema[field];

  if (type === 'string') {

    if (_.isString(val) && modifier !== '$ne') {
      // Replace Percent Signs, work in a case insensitive fashion by default
      val = utils.caseInsensitive(val);
      val = val.replace(/%/g, '.*');
      val = new RegExp('^' + val + '$', 'i');
      return val;
    }

  }

  return cast(val, type);
};


/**
 * Parse Sort
 *
 * @param original
 * @returns {*}
 */
Query.prototype.parseSort = function parseSort(original) {
  "use strict";
  return _.reduce(original, function (sort, order, field) {
    // Normalize id, if used, into _id
    if (field === 'id') field = '_id';

    // Handle Sorting Order with binary or -1/1 values
    sort[field] = ([0, -1].indexOf(order) > -1) ? -1 : 1;

    return sort;
  }, {});
};