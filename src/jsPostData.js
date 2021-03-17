/*globals ObjectRow,Environment,sqlFun */
/**
 * Manages data storing
 * @module PostData
 */
'use strict';

const dsSpace = require('jsDataSet');
const DataSet = dsSpace.DataSet;

/**
 * @type {DataRow}
 */
const DataRow = dsSpace.DataRow;

const DataTable = dsSpace.DataTable;

const jsMultiSelect = require('jsMultiSelect');
const DataAccess = require('jsDataAccess').DataAccess;
const Select = jsMultiSelect.Select;
const isolationLevels = require('jsDataAccess').isolationLevels;
const dataRowState = dsSpace.dataRowState;

const _ = require('lodash');

/**
 *
 * @type Deferred
 */
const Deferred = require("jsDeferred");

const async = require('async');

/**
 * Manages a cache of select max done in a transaction
 * @class MaxCacher
 * @method MaxCacher
 * @param {DataAccess} conn
 * @param {Environment} environment
 * @constructor
 */
function MaxCacher(conn,environment){
  /**
   * Cache of all calculated max
   * @property allMax
   * @private
   * @type {hash}
   */
  this.conn = conn;
  this.formatter = conn.getFormatter();
  this.environment= environment;
  this.allMax = {};
}
MaxCacher.prototype = {
  constructor: MaxCacher
};

/**
 * Gets a hash to retrieve a select max
 * @method getHash
 * @private
 * @param {string} table
 * @param {string} column
 * @param {sqlFun} filter
 * @param {sqlFun} expr
 */
MaxCacher.prototype.getHash = function(table, column,filter,expr){
  return table+'§'+ column+'§'+this.formatter.conditionToSql(filter, this.environment)+'§'+
    this.formatter.toSql(expr);
};


/**
 * Get the max for an expression eventually getting it from cache or giving null if reasonably there is no row
 *  on that match the filter
 *  If there is no selector, then the result can be taken from cache if a query with same parameters has already
 *   be done.
 * @method getMax
 * @param {ObjectRow} r objectRow for which evaluate the max
 * @param {string} column  column to evaluate
 * @param {string[]} selectors //selector fields for the calculation
 * @param {sqlFun} filter //filter to apply
 * @param {sqlFun} expr //expression to evaluate
 * @return {number}
 */
MaxCacher.prototype.getMax = function (r, column, selectors, filter, expr) {
  const def = Deferred(),
      that = this,
      table = r.getRow().table,
      k = this.getHash(table.name, column, filter, expr);
  if (this.allMax[k]){
    def.resolve(this.allMax[k]);
    return def.promise();
  }
  const keySelectors = _.intersection(selectors, table.key()); //fields both key and selector
  if (keySelectors.length > 0) {
    if (_.find(table.dataset.relationsByChild[table.name], function (parentRel) {
      const parentRows = parentRel.getParents(r);
      if (parentRows.length !== 1) {
        return false;
      }
      const parentRow = parentRows[0];
      if (parentRow.getRow().state !== dataRowState.added) {
        return false;
      }
      return ( _(_.intersection(parentRel.childCols, keySelectors))
        .map(function (col) {
          return parentRel.parentCols[_.indexOf(parentRel.childCols, col)];
        })
        .find(function (parentCol) {
          return table.dataset.tables[parentRel.parentTable].autoIncrement(parentCol) !== undefined;
        }) !== undefined);

    })) {
      //if such a relation is found, return null to mean that there is no row on db satisfying that condition,
      // so the max can be evaluated basing on in-memory informations (for example a cached value)
      def.resolve(null);
      return def.promise();
    }
  }
  this.conn.readSingleValue({tableName: table.name, expr:expr, filter:filter, environment:this.environment})
    .done(function(res){
      if (res===undefined){
        res=null;
      }
      that.allMax[k]=res; //put the result in cache
      def.resolve(res);
    })
    .fail(function(err){
      def.reject(err);
    });
  return def.promise();
};

/**
 *
 * @param {ObjectRow} r
 * @param {string} column
 * @param {sqlFun} filter
 * @param {sqlFun} expr
 * @param {object} value
 */
MaxCacher.prototype.setMax = function (r, column, filter, expr, value) {
  this.allMax[this.getHash(r.getRow().table.name, column, filter, expr)] = value;
};


/**
 * Saves a DataSet using a given DataAccess
 * @class PostData
 * @method PostData
 * @param {DataAccess} conn
 * @param {Environment} environment
 * @constructor
 */
function PostData(conn, environment){
  this.conn= conn;
  this.environment = environment;
  if (conn) {
    this.sqlConn = conn.sqlConn;
  }
}

PostData.prototype = {
  constructor: PostData
};

/**
 * Compose a list of all tables that satisfy the checkFunction
 * checkFunction is called with 2 parameters: table, list where
 * table is the current evaluated DataTable,
 * list is a hash of tables already added to list, where the key is the name of the DataTable
 * Elements are added to list when checkFunction returns true
 * @param {DataSet} d
 * @param checkFunction
 * @return {DataTable[]}
 */
PostData.prototype.sortTables = function(d, checkFunction){
  const result = [],
      hash = {};
  let added = true;

  function check(t){
    if (hash[t.name]) {
      return;
    }
    if (checkFunction(d, t.name, hash)) {
      result.push(t);
      hash[t.name] = true;
      added = true;
    }
  }

  while (added){
    added = false;
    _.forEach(d.tables, check);
  }
  return result;

};


/**
 * check if t is a child table of some table not present in allowed and that have changes
 * @param {DataSet} ds
 * @param {string} tableName
 * @param {object} allowedParents
 * @return boolean
 */
PostData.prototype.checkIsNotChild = function (ds, tableName, allowedParents){
  const foundRel = _.find(ds.relationsByChild[tableName], function (rel) {
    if (allowedParents[rel.parentTable]) {
      return false; //child relation has NOT been found
    }
    if (!ds.tables[rel.parentTable].hasChanges()) {
      return false; //parentTable has  no change, relation is allowed
    }
    return true;
  });
  return  (foundRel === undefined);
};

/**
 * check if t is a parent table of some table not present in allowed and that has rows
 * @param {DataSet} ds
 * @param {string} tableName
 * @param {string[]} allowedChilds
 * @return boolean
 */
PostData.prototype.checkIsNotParent = function(ds, tableName, allowedChilds) {
  const foundRel = _.find(ds.relationsByParent[tableName], function (rel) {
    if (allowedChilds[rel.childTable]) {
      return false; //parent relation has NOT been found
    }
    if (ds.tables[rel.childTable].rows.length === 0) {
      return false; //childTable is empty so  relation is allowed
    }
    return true;
  });

  return  (foundRel === undefined);
};

/**
 * Returns modified rows of given tables filtering by rowState
 * @method getTableOps
 * @param {DataTable[]} tables
 * @param {DataRowState|string} rowState
 */
function getTableOps(tables, rowState) {
  return _.reduce(tables, function (list, t) {
    let res = _.filter(t.rows,
        function (r) {
          return (r.getRow().state === rowState);
        }
    );
    if (t.postingOrder) {
      res = _.sortBy(res, t.postingOrder);
    }
    return list.concat(res);
  }, []);


}


/**
 *  Evaluates the list of the changes to apply to the DataBase, in the order they should be "reasonably" done:
 *  All operation on None
 *  Deletes on  Child first, Parent last
 *  Insert on Parent first, Child last
 *  Updates on Parent first, Child last
 *  If a dataTable has a 'postingOrder' property, it is used to sort rows of that table
 * @param {DataSet} original
 */
PostData.prototype.changeList = function(original) {
  const parentFirst = this.sortTables(original, this.checkIsNotChild),
      childFirst = this.sortTables(original, this.checkIsNotParent);
  return _.map(getTableOps(childFirst, dataRowState.deleted).concat(
    getTableOps(parentFirst, dataRowState.added),
    getTableOps(parentFirst, dataRowState.modified)
  ));
};


function defaultCallChecks(post){
  const def = Deferred();

  const res = {checks: [], shouldContinue: true};
  def.resolve(res);
  return def.promise();
}


function defaultAddError(msg) {
  return {msg:msg};
}


/**
 * Saves a dataSet and return empty list if successful or a list of messages if they have to be verified before
 *  effectively committing changes.
 * @method doPost
 * @param {ObjectRow[]} changedRows
 * @param {object} options
 * @param {function} options.getChecks : function(post) this function, when provided, is called before and after
 *  applying changes to db. The first time is called with post=false and the second time with post=true.
 *  Should give a list of messages to return to client. If it does return an empty array, the transaction is committed.
 *  If it does return some rows, those are merged and returned to client, and transaction is roll-backed.
 *  The object returned is {shouldContinue:boolean, checks:array of ProcedureMessage}
 *  if shouldContinue is false in the first call, the procedure terminates and no data is written at all
 *  @param   {function} options.getError  (msg) : function called to add a blocking error, when a db error happens
 *  @param   {string} [options.isolationLevel = DataAccess.isolationLevels.readCommitted]
 *  @param {function} [options.log]  function called with a DataAccess and changedRows as parameters in order to do eventual
 *   db transaction logging
 * @param {function} [options.doUpdate] function called with a DataAccess and changedRows in order to do eventual additionally
 *   updates when it's sure the transaction is to be committed, i.e., no check have been raised. This function must
 *    return an array of checks. If it is empty the transaction is committed otherwise is rollbacked
 * @param {OptimisticLocking} [options.optimisticLocking]
 * @return {object}   {checks:ProcedureMessage[], data:DataSet}
 */
PostData.prototype.doPost = function(changedRows, options) {
  const opt = options || {},
      checks = [],
      that = this;
  let opened = false,
      tranOpen = false,
      terminated = false;
  const result = {checks: checks};
  /**
   * @var def
   * @type Deferred
   */
  const def = Deferred();
  _.defaults(opt, {
    getChecks: defaultCallChecks,
    getError: defaultAddError,
    isolationLevel: isolationLevels.readCommitted
  });

  function appendArray(array1, array2) {
    array1.push.apply(array1, array2);
  }

  if (changedRows.length === 0) {
    def.resolve({checks: []}); //default response on unchanged dataset
    return def.promise();
  }
  // Resets all evaluated cached max
  this.maxCacher = new MaxCacher(this.conn, this.environment);

  /**
   * Adds an error message to output result
   * @method @dbError
   * @param msg
   */
  function dbError(msg) {
    checks.push(opt.getError(msg));
  }

  /**
   * Resolve the promise with result, and do the necessary cleanup: rollback transaction if a transaction is open,
   *  and close connection if the connection is open. If further errors arise, add them to checks as dbErrors
   * @method resolve
   * @returns {Deferred}
   */
  function resolve() {
    terminated = true;
    if (tranOpen) {
      that.conn.rollback()
          .then(function () {
            return that.conn.close();
          })
          .then(function () {
            def.resolve(result);
          })
          .fail(function (err) {
            that.conn.close()
                .done(function () {
                  dbError(err);
                  def.resolve(result);
                });
          });
      return def.promise();
    }
    if (opened) {
      that.conn.close()
          .done(function () {
            def.resolve(result);
          });
    } else {
      def.resolve(result);
    }
    return def.promise();
  }


  // CALL PRE - CHECKS
  opt.getChecks(false)
      .then(function (preChecks) {
        appendArray(checks, preChecks.checks);
        if (preChecks.shouldContinue === false) {
          return resolve();
        }
        //open connection
        return that.conn.open();
      })
      .then(function () {
        if (terminated) {
          return def;
        }
        opened = true;
        //begin transaction
        return that.conn.beginTransaction(opt.isolationLevel);
      })
      .then(function () {
        if (terminated) {
          return def;
        }
        tranOpen = true;
        return that.physicalPostBatch(changedRows, opt.optimisticLocking);
      })
      .then(function () {
        if (terminated) {
          return def;
        }
        if (opt.log) { //optional transaction log
          return opt.log(that.conn, changedRows);
        }
        return Deferred().resolve(true).promise();
      })
      .then(function () {
        if (terminated) {
          return def;
        }
        //CALL POST-CHECK
        return opt.getChecks(true);
      })
      .then(function (postChecks) {
        if (terminated) {
          return def;
        }
        appendArray(checks, postChecks.checks);
        if (checks.length > 0) {
          return resolve(); //forces a rollback
        }
        if (opt.doUpdate) { //optional external post-updating
          return opt.doUpdate(that.conn, changedRows);
        }
        return Deferred().resolve([]).promise();
      })
      .then(function (extChecks) {
        if (terminated) {
          return def;
        }
        appendArray(checks, extChecks.checks);
        if (checks.length > 0) {
          return resolve(); //forces a rollback
        }
        //all is fine so we can do a commit
        return that.conn.commit();
      })
      .then(function (res) {
        if (terminated) {
          return def;
        }
        tranOpen = false;
        _.forEach(changedRows,
            /**
             * @param {ObjectRow} r
             */
            function (r) {
              r.getRow().acceptChanges();
            });
        return that.reselectAllViews(_.filter(changedRows, function (r) {
          return r.getRow !== undefined;
        }));
      })
      .then(function () {
        if (terminated) {
          return def;
        }
        result.data = changedRows;
        resolve(); //closes connection and returns data
      })
      //any fail should reach here
      .fail(function (err) {
        dbError(err);
        resolve();
      });

  return def.promise();
};

/**
 * Get a sql command to select all changed rows that belongs to view tables. Views are identified as tables
 *  having a name different from tableForWriting. It's necessary that they have a key to let this method
 *  consider them.
 * @method  getSelectAllViews
 * @private
 * @param {ObjectRow[]} changedRows
 * @returns {Select []}
 */
PostData.prototype.getSelectAllViews = function (changedRows) {
  return _.reduce(changedRows, function (list, r) {
    const table = r.getRow().table;
    if (table.name === table.tableForWriting()) {
      return list;
    }
    if (table.key().length === 0) {
      return list;
    }
    list.push(new Select(_.keys(r))
      .from(table.tableForReading())
        .intoTable(table.name)
        .multiCompare(table.keyFilter(r))
    );
    return list;
  }, []);
};


/**
 * Read all changed view rows from db
 * @method reselectAllViews
 * @param {DataRow[]} changedRows
 * @returns {*}
 */
PostData.prototype.reselectAllViews = function(changedRows){
  const def = Deferred();
  let selectList;
  if (changedRows.length === 0){
    def.resolve();
    return def.promise();
  }
  const ds = changedRows[0].getRow().table.dataset;

  selectList = this.getSelectAllViews(changedRows);

  this.conn.mergeMultiSelect(selectList, ds)
    .done(function(data){
      def.resolve();
    })
    .fail(function(err){
      def.resolve();
    });
  return def.promise();
};

function promiseWaterfall(tasks) {
  return _.reduce(tasks, function (composedPromise, task) {
    return composedPromise.then(task);
  },  Deferred().resolve().promise());  // initial value
}

function promiseParallel(tasks) {
  Deferred.when.apply(Deferred, tasks.map(function (task) {
    return task();
  }));
}



/**
 * Calculates an autoincrement column checking that there is no other rows with that key in table
 * @method calcAutoId
 * @param {ObjectRow} r
 * @param {AutoIncrementColumn} autoIncrementProperty ({DataRow} r, {string} columnName, {DataAccess} conn}
 * @return {promise} //resolves false if customFunction was found, true otherwise
 */
PostData.prototype.calcAutoId = function(r, autoIncrementProperty) {
  const that = this,
      def = Deferred(),
      table = r.getRow().table,
      field = autoIncrementProperty.columnName;

  if (autoIncrementProperty.customFunction){
    autoIncrementProperty.customFunction(r, field, this.conn)
      .done(function(res){
        table.safeAssign(r,field,res);
        def.resolve(false);
      });
    return def.promise();
  }

  const selector = autoIncrementProperty.getSelector(r),
      fieldExpr = autoIncrementProperty.getExpression(r),
      prefix = autoIncrementProperty.getPrefix(r);


  this.maxCacher.getMax(r, field, autoIncrementProperty.selector, selector, fieldExpr)
    .then(function (res) {
      let foundID, newID;
      if (res === null || res === undefined) {
        newID = 1;
      } else {
        newID = res+1;
      }
      that.maxCacher.setMax(r, field, selector, fieldExpr, newID);
      if (! autoIncrementProperty.isNumber){
        newID = newID.toString();
        while (newID.length < autoIncrementProperty.idLen) {
          newID = '0' + newID;
        }
        newID = prefix + newID;
      }
      table.safeAssign(r, field, newID);
      def.resolve(true);
    });
  return def.promise();
};

/**
 * Calculates all autoincrement property of a DataRow r
 * @param {ObjectRow} r
 * @returns {promise} return true if NO custom autoincrement was found
 */
PostData.prototype.calcAllAutoId = function(r) {
  const that = this,
      def = Deferred(),
      table = r.getRow().table;
  if (r.getRow().state !== dataRowState.added){
    def.resolve(true);
    return def.promise();
  }
  Deferred.when.apply(Deferred,
    _.map(table.getAutoIncrementColumns(), function (col) {
    return that.calcAutoId(r, table.autoIncrement(col));
  }))
    .done(function(){
      const arr = Array.prototype.slice.call(arguments);
      def.resolve(_.every(arr,function(el){return el}));
    });
  return def.promise();
};

 /**
  * Gets an array of changed rows and returns a sequence of sql command that does the post
  * @method getSqlStatements
  * @param {DataRow[]}changedRows
  * @param {OptimisticLocking} optimisticLocking
  * @returns {string}
  */
 PostData.prototype.getSqlStatements = function (changedRows, optimisticLocking){
   const def = Deferred(),
       that = this;
   let internalIndex = 0,
       rows = [],
       sql,
       failed = false;

   /**
    *
    * @param {ObjectRow} preparedRow
    * @param {boolean} hasCustomAutoincrement
      */
   function sqlMerge(preparedRow, hasCustomAutoincrement){
     if (failed){
       return;
     }
     rows.push(preparedRow);
     const cmd = that.conn.getPostCommand(preparedRow, optimisticLocking, that.environment),
         errCmd = that.sqlConn.giveErrorNumberDataWasNotWritten(internalIndex);
     if (sql) {
       sql = that.sqlConn.appendCommands([sql, cmd, errCmd]);
     } else {
       sql = that.sqlConn.appendCommands([cmd, errCmd]);
     }
     if (sql.length > that.sqlSizeLimit || hasCustomAutoincrement){
       def.notify(rows,sql);
       internalIndex = 0;
       rows = [];
       sql = '';
     } else {
       internalIndex += 1;
     }
   }

   async.eachSeries(changedRows, function(r,callback){
       that.calcAllAutoId(r)
         .done(function(noCustomAutoincrementFound){
           sqlMerge(r, !noCustomAutoincrementFound);
           callback(null);
         })
        .fail(function(err){
           def.reject(err);
           failed=true;
           callback(err);
         });
   },
   function(err){
     if (err) {
       failed = true;
       def.reject(err);
     }
     if (rows.length>0){
       def.notify(rows, sql);
     }
     def.resolve();
   });

  return def.promise();
};



PostData.prototype.sqlSizeLimit = 4000;

 /**
  * Reads again a row from db
  * @method reselect
  * @param {ObjectRow} row
  * @returns {*}
  */
 PostData.prototype.reselect = function(row) {
   const table = row.getRow().table;
   row.getRow().rejectChanges();
   return this.conn.selectIntoTable({
     table: table,
     filter: table.keyFilter(row),
     environment: this.environment
   });

 };

 /**
  * Runs a sequence of db commands in order to save an array of rows
  * @method physicalPostBatch
  * @param {ObjectRow[]}changedRows
  * @param {OptimisticLocking} optimisticLocking
  * @returns {*} Resolved promise if all ok, rejected promise if errors
  */
PostData.prototype.physicalPostBatch = function(changedRows, optimisticLocking){
  const def = Deferred(),
      that = this;
  let sqlCmdLaunched = 0,
      sqlCmdRun = 0,
      endOfCmdReached = false,
      failed = false;

  function sqlCmdRunner(rows,sql){
    const sqlComplete = that.sqlConn.appendCommands([sql, that.sqlConn.giveConstant(-1)]);
    sqlCmdLaunched += 1;
    that.conn.runCmd(sqlComplete)
      .done(function(res){

        if (res === -1){
          sqlCmdRun += 1;
          if (endOfCmdReached){
            def.resolve();
          }
          return;
        }
        if (res <0 || res >= rows.length){
          def.reject('internal error running command ' + sqlComplete);
          return;
        }

        const row = rows[res],
            sqlErrorCmd = that.conn.getPostCommand(row, optimisticLocking, that.environment);
        if (row.getRow().state === dataRowState.modified){
          that.reselect(row)
            .done(function(){
              def.reject('error running command' + sqlErrorCmd);
            })
            .fail(function(){
              def.reject('error running command' + sqlErrorCmd);
            })
        } else {
          def.reject('error running command' + sqlErrorCmd);
        }
      })
      .fail(function(err){
        failed=true;
        def.reject('Got Error:'+err+' running '+ sqlComplete);
      })

  }

  that.getSqlStatements(changedRows, optimisticLocking)
    .progress(function(rows, sql){
      if (failed) {
        return;
      }
      sqlCmdRunner(rows,sql);
    })
    .done(function(sqlCmdArray){
      if (failed) {
        return;
      }
      endOfCmdReached=true;
      if (sqlCmdLaunched=== sqlCmdRun){
        def.resolve();
      }
    })
    .fail(function(err){
      if (failed) {
        return;
      }
      def.reject(err);
    });


  return def.promise();
};

module.exports = {
  PostData: PostData,
  MaxCacher: MaxCacher
};
