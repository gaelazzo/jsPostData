/*globals describe, beforeEach,it,expect,jasmine,spyOn */

/*jshint -W069 */

var PostData = require('../../src/jsPostData').PostData,
    MaxCacher = require('../../src/jsPostData').MaxCacher,
    dsNameSpace = require('jsDataSet'),
    dq = require('jsDataQuery'),
    DA = require('jsDataAccess');
/**
 *
 * @type {Deferred}
 */
var    Deferred = require("jsDeferred");
var Environment       = require('../fakeEnvironment'),
    dbList            = require('jsDbList'),
    dataRowState      = dsNameSpace.dataRowState,
    DataSet           = dsNameSpace.DataSet,
    Select            = require('jsMultiSelect').Select,
    OptimisticLocking = dsNameSpace.OptimisticLocking,
    fs                = require('fs'),
    path              = require('path'),
    _                 = require('lodash');



/**
 * *****************************************************************************************
 * VERY IMPORTANT VERY IMPORTANT VERY IMPORTANT VERY IMPORTANT VERY IMPORTANT VERY IMPORTANT
 * *****************************************************************************************
 * It's necessary, before start running the test, to create a file templated like:
 *  { "server": "db server address",
 *    "dbName": "database name",  //this must be an EMPTY database
 *    "user": "db user",
 *    "pwd": "db password"
 *  }
 */
//PUT THE  FILENAME OF YOUR FILE HERE:

var configName = path.join('test', 'db.json');
if (process.env.TRAVIS){
    dbConfig = { "server": "127.0.0.1",
        "dbName": "test",
        "user": "root",
        "pwd": ""
    };
}
else {
    dbConfig = JSON.parse(fs.readFileSync(configName).toString());
}

/**
 * setup the dbList module
 */
dbList.init({
    encrypt: false,
    decrypt: false,
    encryptedFileName: 'test/dbList.bin'
});

var good = {
    server: dbConfig.server,
    useTrustedConnection: false,
    user: dbConfig.user,
    pwd: dbConfig.pwd,
    database: dbConfig.dbName,
    sqlModule: 'jsMySqlDriver'
};


describe('setup dataBase', function () {
    var sqlConn;
    beforeEach(function (done) {
        dbList.setDbInfo('test', good);
        sqlConn = dbList.getConnection('test');
        sqlConn.open().
            done(function () {
                done();
            });
    });

    afterEach(function () {
        if (sqlConn) {
            sqlConn.destroy();
        }
        sqlConn = null;
    });


    it('should run the setup script', function (done) {
        sqlConn.run(fs.readFileSync(path.join('test', 'setup.sql')).toString())
            .done(function () {
                expect(true).toBeTruthy();
                done();
            })
            .fail(function (res) {
                expect(res).toBeUndefined();
                done();
            });
    }, 30000);

});

describe('PostData', function () {
'use strict';
    var DAC, env;


    describe('changeList', function () {
        var d;
        beforeEach(function (done) {
            var i, t, rowCount, row;
            d = new DataSet('D');
            i = 11;
            while (--i > 0) {
                t = d.newTable('tab' + i);
                t.columns.push('id' + i);
                t.columns.push('data' + i);
                if (i < 10) {
                    t.columns.push('idExt' + (i + 1));
                    d.newRelation('r' + i + 'a', 'tab' + i, ['idExt' + (i + 1)], 'tab' + (i + 1), ['id' + (i + 1)]);
                }
                if (i < 9) {
                    t.columns.push('idExt' + (i + 2));
                }
                if (i < 8) {
                    t.columns.push('idExt' + (i + 3));
                }
                rowCount = 0;
                while (++rowCount < 6) {
                    row = t.newRow({
                        'id': rowCount,
                        'data': 'about' + rowCount,
                        'idExt': 2 * rowCount,
                        'tabRif': t.name
                    });
                }
            }

            DAC = undefined;
            env = new Environment();

            dbList.getDataAccess('test')
                .done(function (conn) {
                    DAC = conn;
                    done();
                })
                .fail(function (err) {
                    done();
                });
        });


        it('should be a function', function () {
            expect(PostData.prototype.changeList).toEqual(jasmine.any(Function));
        });

        it('should return an array', function () {
            expect(PostData.prototype.changeList(d)).toEqual(jasmine.any(Array));
        });


        it('should return as many rows as there were modified in d', function () {
            var p = new PostData(DAC),
                res = p.changeList(d);
            expect(res.length).toBe(50);
        });


        it('should return as many rows as there were modified in d (2th set)', function () {
            d.tables.tab1.acceptChanges();
            d.tables.tab3.clear();
            var p = new PostData(),
                res = p.changeList(d);
            expect(res.length).toBe(40);
        });

        it('should return as many rows as there were modified in d (3th set)', function () {
            d.tables.tab4.rejectChanges();
            var p = new PostData(),
                res = p.changeList(d);
            expect(res.length).toBe(45);
        });

        it('should return as many rows as there were modified in d (4th set)', function () {
            d.acceptChanges();
            var p = new PostData(),
                res = p.changeList(d);
            expect(res.length).toBe(0);
        });

        it('should return as many rows as there were modified in d (5th set)', function () {
            d.tables.tab4.acceptChanges();
            d.tables.tab6.acceptChanges();
            d.tables.tab8.acceptChanges();
            d.tables.tab9.acceptChanges();
            var i = 0;
            d.tables.tab4.rows[0].getRow().del();
            d.tables.tab4.rows[3].getRow().del();
            d.tables.tab4.rows[2].data4 = 'ciao';
            d.tables.tab4.rows[4].data4 = 'ciao';

            d.tables.tab8.rows[0].getRow().del();
            d.tables.tab8.rows[1].getRow().del();
            d.tables.tab8.rows[2].data8 = 'hi';
            d.tables.tab8.rows[4].data8 = 'hi';

            d.tables.tab9.rows[1].data9 = 'sayonara';
            d.tables.tab9.rows[2].data9 = 'sayonara';

            var p = new PostData(),
                res = p.changeList(d);
            expect(res.length).toBe(40);
        });

        it('should return as many rows as there were modified in d (6th set)', function () {
            d.tables.tab4.acceptChanges();
            d.tables.tab6.acceptChanges();
            d.tables.tab8.acceptChanges();
            d.tables.tab9.acceptChanges();
            var i = 0;
            d.tables.tab4.rows[0].getRow().del();
            d.tables.tab4.rows[1].getRow().del();
            d.tables.tab4.rows[2].getRow().del();
            d.tables.tab4.rows[3].getRow().del();
            d.tables.tab4.rows[4].getRow().del();

            d.tables.tab8.rows[0].getRow().del();
            d.tables.tab8.rows[1].getRow().del();
            d.tables.tab8.rows[2].data8 = 'hi';
            d.tables.tab8.rows[4].data8 = 'hi';

            d.tables.tab9.rows[1].data9 = 'sayonara';
            d.tables.tab9.rows[2].data9 = 'sayonara';

            var p = new PostData(),
                res = p.changeList(d);
            expect(res.length).toBe(41);
        });

        it('checkIsNotParent should return not-parents', function () {
            d.tables.tab4.acceptChanges();
            d.tables.tab6.acceptChanges();
            d.tables.tab8.acceptChanges();
            d.tables.tab9.acceptChanges();
            var i = 0;
            d.tables.tab4.rows[0].getRow().del();
            d.tables.tab4.rows[1].getRow().del();
            d.tables.tab4.rows[2].getRow().del();
            d.tables.tab4.rows[3].getRow().del();
            d.tables.tab4.rows[4].getRow().del();

            d.tables.tab8.rows[0].getRow().del();
            d.tables.tab8.rows[1].getRow().del();
            d.tables.tab8.rows[2].data8 = 'hi';
            d.tables.tab8.rows[4].data8 = 'hi';

            d.tables.tab9.rows[1].data9 = 'sayonara';
            d.tables.tab9.rows[2].data9 = 'sayonara';

            var p = new PostData();
            expect(p.checkIsNotParent(d, 'tab4', {})).toBeFalsy();
            expect(p.checkIsNotParent(d, 'tab4', {tab5: true})).toBeTruthy();
            expect(p.checkIsNotParent(d, 'tab5', {})).toBeFalsy();
            expect(p.checkIsNotParent(d, 'tab5', {tab6: true})).toBeTruthy();
            expect(p.checkIsNotParent(d, 'tab10', {})).toBeTruthy();
            expect(p.checkIsNotParent(d, 'tab10', {tab11: true})).toBeTruthy();
            expect(p.checkIsNotParent(d, 'tab1', {})).toBeFalsy();
            expect(p.checkIsNotParent(d, 'tab1', {tab2: true})).toBeTruthy();
            d.tables.tab2.clear();
            expect(p.checkIsNotParent(d, 'tab1', {})).toBeTruthy();
            d.tables.tab5.acceptChanges();
            expect(p.checkIsNotParent(d, 'tab4', {})).toBeFalsy();
            d.tables.tab5.clear();
            expect(p.checkIsNotParent(d, 'tab4', {})).toBeTruthy();
        });

        it('checkIsNotChild should return not-childs', function () {
            d.tables['tab4'].acceptChanges();
            d.tables['tab6'].acceptChanges();
            d.tables['tab8'].acceptChanges();
            d.tables['tab9'].acceptChanges();
            var i = 0;
            d.tables['tab4'].rows[0].getRow().del();
            d.tables['tab4'].rows[1].getRow().del();
            d.tables['tab4'].rows[2].getRow().del();
            d.tables['tab4'].rows[3].getRow().del();
            d.tables['tab4'].rows[4].getRow().del();

            d.tables['tab8'].rows[0].getRow().del();
            d.tables['tab8'].rows[1].getRow().del();
            d.tables['tab8'].rows[2].data8 = 'hi';
            d.tables['tab8'].rows[4].data8 = 'hi';

            d.tables['tab9'].rows[1].data9 = 'sayonara';
            d.tables['tab9'].rows[2].data9 = 'sayonara';

            var p = new PostData();
            expect(p.checkIsNotChild(d, 'tab4', {})).toBeFalsy();
            expect(p.checkIsNotChild(d, 'tab4', {tab3: true})).toBeTruthy();
            expect(p.checkIsNotChild(d, 'tab5', {})).toBeFalsy();
            expect(p.checkIsNotChild(d, 'tab5', {tab4: true})).toBeTruthy();
            expect(p.checkIsNotChild(d, 'tab10', {})).toBeFalsy();
            expect(p.checkIsNotChild(d, 'tab10', {tab9: true})).toBeTruthy();
            expect(p.checkIsNotChild(d, 'tab1', {})).toBeTruthy();
            expect(p.checkIsNotChild(d, 'tab1', {tab2: true})).toBeTruthy();
            d.tables.tab9.acceptChanges();
            expect(p.checkIsNotChild(d, 'tab10', {})).toBeTruthy();
            d.tables.tab3.acceptChanges();
            expect(p.checkIsNotChild(d, 'tab4', {})).toBeTruthy();
            d.tables.tab3.clear();
            expect(p.checkIsNotChild(d, 'tab4', {})).toBeTruthy();
        });

        it('sortTables should work called with checkIsNotChild', function () {
            d.tables.tab4.acceptChanges();
            d.tables.tab6.acceptChanges();
            d.tables.tab8.acceptChanges();
            d.tables.tab9.acceptChanges();
            var i = 0;
            d.tables.tab4.rows[0].getRow().del();
            d.tables.tab4.rows[1].getRow().del();
            d.tables.tab4.rows[2].getRow().del();
            d.tables.tab4.rows[3].getRow().del();
            d.tables.tab4.rows[4].getRow().del();

            d.tables.tab8.rows[0].getRow().del();
            d.tables.tab8.rows[1].getRow().del();
            d.tables.tab8.rows[2].data8 = 'hi';
            d.tables.tab8.rows[4].data8 = 'hi';

            d.tables.tab9.rows[1].data9 = 'sayonara';
            d.tables.tab9.rows[2].data9 = 'sayonara';
            var p = new PostData(),
                res = p.sortTables(d, p.checkIsNotChild);
            // tab 10, 9, 8 are children. Tab7 is not because tab6 is unchanged
            //we have inserted tables in reverse order so tab7 comes before than tab1. After putting tab7, comes
            // tab8 because at that point, tab7 is an allowed parent. Also comes tab2, because tab1 is allowed as
            // parent at that point. Then come tab9 and tab3 because tab2 and tab8 are then allowed parents.
            // Then come tab10 and tab4, then tab5 and finally tab6.
            expect(_.map(res, 'name')).toEqual(['tab7', 'tab1', 'tab8', 'tab2', 'tab9', 'tab3', 'tab10', 'tab4', 'tab5', 'tab6']);

        });

        it('sortTables should work called with checkIsNotParent', function () {
            d.tables.tab4.acceptChanges();
            d.tables.tab6.acceptChanges();
            d.tables.tab8.acceptChanges();
            d.tables.tab9.acceptChanges();
            var i = 0;
            d.tables.tab4.rows[0].getRow().del();
            d.tables.tab4.rows[1].getRow().del();
            d.tables.tab4.rows[2].getRow().del();
            d.tables.tab4.rows[3].getRow().del();
            d.tables.tab4.rows[4].getRow().del();
            d.tables.tab5.clear();
            d.tables.tab8.rows[0].getRow().del();
            d.tables.tab8.rows[1].getRow().del();
            d.tables.tab8.rows[2].data8 = 'hi';
            d.tables.tab8.rows[4].data8 = 'hi';

            d.tables.tab9.rows[1].data9 = 'sayonara';
            d.tables.tab9.rows[2].data9 = 'sayonara';
            var p = new PostData(),
                res = p.sortTables(d, p.checkIsNotParent);
            // tab 10 is not parent, then comes tab 9 cause tab10 now is an allowed parent then tab8 and so on
            expect(_.map(res, 'name')).toEqual(['tab10', 'tab9', 'tab8', 'tab7', 'tab6', 'tab5', 'tab4', 'tab3', 'tab2', 'tab1']);

        });

        it('sortTables should work called with checkIsNotParent (2th set)', function () {
            d.newRelation('rel000', 'tab6', 'extid6', 'tab3', 'id3');
            d.newRelation('rel001', 'tab10', 'extid1', 'tab1', 'id1');
            d.tables.tab4.acceptChanges();
            d.tables.tab6.acceptChanges();
            d.tables.tab8.acceptChanges();
            d.tables.tab9.acceptChanges();
            var i = 0;
            d.tables.tab4.rows[0].getRow().del();
            d.tables.tab4.rows[1].getRow().del();
            d.tables.tab4.rows[2].getRow().del();
            d.tables.tab4.rows[3].getRow().del();
            d.tables.tab4.rows[4].getRow().del();
            d.tables.tab5.clear();
            d.tables.tab8.rows[0].getRow().del();
            d.tables.tab8.rows[1].getRow().del();
            d.tables.tab8.rows[2].data8 = 'hi';
            d.tables.tab8.rows[4].data8 = 'hi';

            d.tables.tab9.rows[1].data9 = 'sayonara';
            d.tables.tab9.rows[2].data9 = 'sayonara';
            var p = new PostData(),
                res = p.sortTables(d, p.checkIsNotParent);
            //they are all parents but tab4 because tab5 is empty. So then tab4 becomes an allowed parent and tab3 can follow
            // then tab2 and tab1. At this point, tab1 is an allowed parent for tab10 so comes tab10, then tab8,tab7,6,5
            expect(_.map(res, 'name')).toEqual(['tab4', 'tab3', 'tab2', 'tab1', 'tab10', 'tab9', 'tab8', 'tab7', 'tab6', 'tab5']);

        });

        it('insert on parents should precede insert on child', function () {
            d.tables.tab4.acceptChanges();
            d.tables.tab6.acceptChanges();
            d.tables.tab8.acceptChanges();
            d.tables.tab9.acceptChanges();
            d.tables.tab4.rows[0].getRow().del();
            d.tables.tab4.rows[1].getRow().del();
            d.tables.tab4.rows[2].getRow().del();
            d.tables.tab4.rows[3].getRow().del();
            d.tables.tab4.rows[4].getRow().del();
            d.tables.tab5.clear();
            d.tables.tab8.rows[0].getRow().del();
            d.tables.tab8.rows[1].getRow().del();
            d.tables.tab8.rows[2].data8 = 'hi';
            d.tables.tab8.rows[4].data8 = 'hi';

            d.tables.tab9.rows[1].data9 = 'sayonara';
            d.tables.tab9.rows[2].data9 = 'sayonara';
            var i = 0,
                j,
                p = new PostData(),
                res = p.changeList(d),
                childFirst = _.map(p.sortTables(d, p.checkIsNotParent), 'name'),
                parentFirst = _.map(p.sortTables(d, p.checkIsNotChild), 'name');


            for (i = 0; i < res.length; i++) {
                if (res[i].getRow().state !== dataRowState.added) {
                    continue;
                }
                for (j = i + 1; j < res.length; j++) {
                    if (res[j].getRow().state !== dataRowState.added) {
                        continue;
                    }
                    var posI = _.indexOf(parentFirst, res[i].tabRif),
                        posJ = _.indexOf(parentFirst, res[j].tabRif);

                    expect(posI >= 0).toBeTruthy();
                    expect(posJ >= 0).toBeTruthy();
                    expect(posI <= posJ).toBeTruthy();
                }
            }
        });

        it('deletes on child should precede deletes on parent', function () {
            d.tables.tab4.acceptChanges();
            d.tables.tab6.acceptChanges();
            d.tables.tab8.acceptChanges();
            d.tables.tab9.acceptChanges();
            var i = 0;
            d.tables.tab4.rows[0].getRow().acceptChanges();
            d.tables.tab4.rows[1].getRow().acceptChanges();
            d.tables.tab4.rows[4].getRow().acceptChanges();
            d.tables.tab4.rows[0].getRow().del();
            d.tables.tab4.rows[1].getRow().del();
            d.tables.tab4.rows[4].getRow().del();

            d.tables.tab3.rows[0].getRow().acceptChanges();
            d.tables.tab3.rows[1].getRow().acceptChanges();
            d.tables.tab3.rows[3].getRow().acceptChanges();
            d.tables.tab3.rows[4].getRow().acceptChanges();
            d.tables.tab3.rows[0].getRow().del();
            d.tables.tab3.rows[1].getRow().del();
            d.tables.tab3.rows[3].getRow().del();
            d.tables.tab3.rows[4].getRow().del();


            d.tables.tab5.rows[0].getRow().acceptChanges();
            d.tables.tab5.rows[1].getRow().acceptChanges();
            d.tables.tab5.rows[0].getRow().del();
            d.tables.tab5.rows[1].getRow().del();

            d.tables['tab8'].rows[0].getRow().del();
            d.tables['tab8'].rows[1].getRow().del();
            d.tables['tab8'].rows[2].data8 = 'hi';
            d.tables['tab8'].rows[4].data8 = 'hi';

            d.tables['tab9'].rows[1].data9 = 'sayonara';
            d.tables['tab9'].rows[2].data9 = 'sayonara';
            var j,
                p = new PostData(),
                res = p.changeList(d),
                childFirst = _.map(p.sortTables(d, p.checkIsNotParent), 'name'),
                parentFirst = _.map(p.sortTables(d, p.checkIsNotChild), 'name');

            for (i = 0; i < res.length; i++) {
                if (res[i].getRow().state !== dataRowState.deleted) {
                    continue;
                }
                for (j = i + 1; j < res.length; j++) {
                    if (res[j].getRow().state !== dataRowState.deleted) {
                        continue;
                    }

                    var posI = _.indexOf(childFirst, res[i].tabRif),
                        posJ = _.indexOf(childFirst, res[j].tabRif);

                    expect(posI >= 0).toBeTruthy();
                    expect(posJ >= 0).toBeTruthy();
                    expect(posI <= posJ).toBeTruthy();
                }
            }
        });

        describe('deletes should precede other kind of operations', function () {
            it('insert on parents should precede insert on child', function () {
                d.tables.tab4.acceptChanges();
                d.tables.tab6.acceptChanges();
                d.tables.tab8.acceptChanges();
                d.tables.tab9.acceptChanges();
                d.tables.tab4.rows[0].getRow().del();
                d.tables.tab4.rows[1].getRow().del();
                d.tables.tab4.rows[2].getRow().del();
                d.tables.tab4.rows[3].getRow().del();
                d.tables.tab4.rows[4].getRow().del();
                d.tables.tab5.clear();
                d.tables.tab8.rows[0].getRow().del();
                d.tables.tab8.rows[1].getRow().del();
                d.tables.tab8.rows[2].data8 = 'hi';
                d.tables.tab8.rows[4].data8 = 'hi';

                d.tables.tab9.rows[1].data9 = 'sayonara';
                d.tables.tab9.rows[2].data9 = 'sayonara';
                var i = 0,
                    j,
                    p = new PostData(),
                    res = p.changeList(d),
                    childFirst = _.map(p.sortTables(d, p.checkIsNotParent), 'name'),
                    parentFirst = _.map(p.sortTables(d, p.checkIsNotChild), 'name');


                for (i = 0; i < res.length; i++) {
                    if (res[i].getRow().state === dataRowState.deleted) {
                        continue;
                    }
                    for (j = i + 1; j < res.length; j++) {
                        if (res[j].getRow().state !== dataRowState.deleted) {
                            continue;
                        }
                        expect(true).toBeFalsy(); //should not reach here
                    }
                }
            });
        });
    });

    describe('MaxCacher', function () {
        var DAC, env;
        beforeEach(function (done) {
            DAC = undefined;
            env = new Environment();

            dbList.getDataAccess('test')
                .done(function (conn) {
                    DAC = conn;
                    done();
                })
                .fail(function (err) {
                    done();
                });
        });

        it('new MaxCacher(conn,environment) should return a class', function () {
            var M = new MaxCacher(DAC, env);
            expect(M).toEqual(jasmine.any(MaxCacher));
        });
        it('getHash should return a string', function () {
            var M = new MaxCacher(DAC, env);
            expect(M.getHash).toEqual(jasmine.any(Function));
            expect(M.getHash()).toEqual(jasmine.any(String));
        });

        it('getHash should return a string different if parameters are different', function () {
            var M = new MaxCacher(DAC, env);
            var table = 'operator',
                column = 'noperator',
                filter = dq.eq('year', 2014),
                expr = dq.max('noperator'),
                expr2 = dq.max('anotherfield'),
                filter2 = dq.eq('year', 2013),
                column2 = 'anotherid',
                table2 = 'anothertable',
                hash1 = M.getHash(table, column, filter, expr),
                hash2 = M.getHash(table2, column, filter, expr),
                hash3 = M.getHash(table, column2, filter, expr),
                hash4 = M.getHash(table, column, filter2, expr),
                hash5 = M.getHash(table, column, filter, expr2);
            expect(hash1).not.toEqual(hash2);
            expect(hash1).not.toEqual(hash3);
            expect(hash1).not.toEqual(hash4);
            expect(hash1).not.toEqual(hash5);
        });

        it('getMax should call conn.readSingleValue null when there is no selector', function (done) {
            var M = new MaxCacher(DAC, env);
            var table = 'operator',
                column = 'noperator',
                filter = dq.eq('year', 2014),
                expr = dq.max('noperator'),
                ds = new DataSet('d'),
                t = ds.newTable('operator'),
                r = t.newRow({});
            spyOn(DAC, 'readSingleValue').andCallFake(function () {
                var def = Deferred().resolve(1);
                return def.promise();
            });
            M.getMax(r, column, null, filter, expr)
                .done(function (res) {
                    expect(DAC.readSingleValue).toHaveBeenCalled();
                    expect(res).toBe(1);
                    done();
                })
                .fail(function (err) {
                    expect(err).toBeUndefined();
                    done();
                });
        });

        it('getMax should not call conn.readSingleValue null when there is no selector the 2th time is called',
            function (done) {
                var M = new MaxCacher(DAC, env);
                var table = 'operator',
                    column = 'noperator',
                    filter = dq.eq('year', 2014),
                    expr = dq.max('noperator'),
                    ds = new DataSet('d'),
                    t = ds.newTable('operator'),
                    r = t.newRow({}),
                    n = 0;
                spyOn(DAC, 'readSingleValue').andCallFake(function () {
                    var def = Deferred().resolve(++n);
                    return def.promise();
                });
                M.getMax(r, column, null, filter, expr)
                    .done(function (res) {
                        expect(res).toBe(1);
                        expect(DAC.readSingleValue.callCount).toEqual(1);
                        M.getMax(r, column, null, filter, expr)
                            .done(function (res) {
                                expect(res).toBe(1);
                                expect(DAC.readSingleValue.callCount).toEqual(1);
                            })
                            .fail(function (err) {
                                expect(err).toBeUndefined();
                                done();
                            });
                        done();
                    })
                    .fail(function (err) {
                        expect(err).toBeUndefined();
                        done();
                    });
            });

        it('getMax should call conn.readSingleValue  when there is selector but parent not added', function (done) {
            var M = new MaxCacher(DAC, env);
            var table = 'operator',
                column = 'noperator',
                filter = dq.eq('year', 2014),
                expr = dq.max('noperator'),
                ds = new DataSet('d'),
                parent = ds.newTable('parent'),
                t = ds.newTable('operator'),
                rParent = parent.newRow({idparent: 1}),
                rChild = t.newRow({idparent: 1});
            parent.acceptChanges();
            parent.key(['idparent']);
            t.key(['idoperator', 'idparent']);
            ds.newRelation('r', 'parent', ['idparent'], 'operator', ['idparent']);
            spyOn(DAC, 'readSingleValue').andCallFake(function () {
                var def = Deferred().resolve(1);
                return def.promise();
            });
            M.getMax(rChild, column, null, filter, expr)
                .done(function (res) {
                    expect(DAC.readSingleValue).toHaveBeenCalled();
                    expect(res).toBe(1);
                    done();
                })
                .fail(function (err) {
                    expect(err).toBeUndefined();
                    done();
                });
        });

        it('getMax SHOULD call relation.getParents  when there is selector but parent not added', function (done) {
            var M = new MaxCacher(DAC, env);
            var table = 'operator',
                column = 'noperator',
                filter = dq.eq('year', 2014),
                expr = dq.max('noperator'),
                ds = new DataSet('d'),
                rel,
                parent = ds.newTable('parent'),
                t = ds.newTable('operator'),
                rParent = parent.newRow({idparent: 1}),
                rChild = t.newRow({idparent: 1});
            parent.acceptChanges();
            parent.key(['idparent']);
            t.key(['idoperator', 'idparent']);
            rel = ds.newRelation('r', 'parent', ['idparent'], 'operator', ['idparent']);
            spyOn(rel, 'getParents').andCallThrough();
            spyOn(DAC, 'readSingleValue').andCallFake(function () {
                var def = Deferred().resolve(1);
                return def.promise();
            });
            M.getMax(rChild, column, ['idparent'], filter, expr)
                .done(function (res) {
                    expect(rel.getParents).toHaveBeenCalled();
                    done();
                })
                .fail(function (err) {
                    expect(err).toBeUndefined();
                    done();
                });
        });

        it('getMax SHOULD NOT call relation.getParents  when there is selector but parent not added - 2th call in a row',
            function (done) {
                var M = new MaxCacher(DAC, env);
                var table = 'operator',
                    column = 'noperator',
                    filter = dq.eq('year', 2014),
                    expr = dq.max('noperator'),
                    ds = new DataSet('d'),
                    rel,
                    parent = ds.newTable('parent'),
                    t = ds.newTable('operator'),
                    rParent = parent.newRow({idparent: 1}),
                    rChild = t.newRow({idparent: 1});
                parent.acceptChanges();
                parent.key(['idparent']);
                t.key(['idoperator', 'idparent']);
                rel = ds.newRelation('r', 'parent', ['idparent'], 'operator', ['idparent']);
                spyOn(rel, 'getParents').andCallThrough();
                spyOn(DAC, 'readSingleValue').andCallFake(function () {
                    var def = Deferred().resolve(1);
                    return def.promise();
                });
                M.getMax(rChild, column, ['idparent'], filter, expr)
                    .done(function (res) {
                        expect(rel.getParents.callCount).toEqual(1);
                        M.getMax(rChild, column, ['idparent'], filter, expr)
                            .done(function (res) {
                                expect(rel.getParents.callCount).toEqual(1);
                                done();
                            })
                            .fail(function (err) {
                                expect(err).toBeUndefined();
                                done();
                            });
                    })
                    .fail(function (err) {
                        expect(err).toBeUndefined();
                        done();
                    });
            });


        it('getMax should NOT call conn.readSingleValue  when there is selector and parent ' +
            'is added and autoincrement',
            function (done) {
                var M = new MaxCacher(DAC, env);
                var table = 'operator',
                    column = 'noperator',
                    filter = dq.eq('year', 2014),
                    expr = dq.max('noperator'),
                    ds = new DataSet('d'),
                    rel,
                    parent = ds.newTable('parent'),
                    t = ds.newTable('operator'),
                    rParent = parent.newRow({idparent: 1}),
                    rChild = t.newRow({idparent: 1});
                parent.key(['idparent']);
                t.key(['idoperator', 'idparent']);
                parent.autoIncrement('idparent', {});
                //t.autoIncrement('noperator', {selector:['idparent']}); //not necessary for the test
                rel = ds.newRelation('r', 'parent', ['idparent'], 'operator', ['idparent']);
                var parRel = rel.getParentsFilter(rChild);
                spyOn(DAC, 'readSingleValue').andCallFake(function () {
                    var def = Deferred().resolve(1);
                    return def.promise();
                });
                M.getMax(rChild, column, ['idparent'], filter, expr)
                    .done(function (res) {
                        expect(DAC.readSingleValue).not.toHaveBeenCalled();
                        expect(res).toBe(null);
                        done();
                    })
                    .fail(function (err) {
                        expect(err).toBeUndefined();
                        done();
                    });
            });


        it('getMax SHOULD call conn.readSingleValue  when there is selector and parent ' +
            'is added but NOT autoincrement',
            function (done) {
                var M = new MaxCacher(DAC, env);
                var table = 'operator',
                    column = 'noperator',
                    filter = dq.eq('year', 2014),
                    expr = dq.max('noperator'),
                    ds = new DataSet('d'),
                    rel,
                    parent = ds.newTable('parent'),
                    t = ds.newTable('operator'),
                    rParent = parent.newRow({idparent: 1}),
                    rChild = t.newRow({idparent: 1});
                parent.key(['idparent']);
                t.key(['idoperator', 'idparent']);
                //parent.autoIncrement('idparent', {});
                //t.autoIncrement('noperator', {selector:['idparent']}); //not necessary for the test
                rel = ds.newRelation('r', 'parent', ['idparent'], 'operator', ['idparent']);
                var parRel = rel.getParentsFilter(rChild);
                spyOn(DAC, 'readSingleValue').andCallFake(function () {
                    var def = Deferred().resolve(100);
                    return def.promise();
                });
                M.getMax(rChild, column, ['idparent'], filter, expr)
                    .done(function (res) {
                        expect(DAC.readSingleValue).toHaveBeenCalled();
                        expect(res).toBe(100);
                        done();
                    })
                    .fail(function (err) {
                        expect(err).toBeUndefined();
                        done();
                    });
            });

        it('getMax SHOULD call conn.readSingleValue  when there is selector and parent ' +
            'is autoincrement but NOT added',
            function (done) {
                var M = new MaxCacher(DAC, env);
                var table = 'operator',
                    column = 'noperator',
                    filter = dq.eq('year', 2014),
                    expr = dq.max('noperator'),
                    ds = new DataSet('d'),
                    rel,
                    parent = ds.newTable('parent'),
                    t = ds.newTable('operator'),
                    rParent = parent.newRow({idparent: 1}),
                    rChild = t.newRow({idparent: 1});
                parent.key(['idparent']);
                parent.acceptChanges();
                t.key(['idoperator', 'idparent']);
                parent.autoIncrement('idparent', {});
                //t.autoIncrement('noperator', {selector:['idparent']}); //not necessary for the test
                rel = ds.newRelation('r', 'parent', ['idparent'], 'operator', ['idparent']);
                var parRel = rel.getParentsFilter(rChild);
                spyOn(DAC, 'readSingleValue').andCallFake(function () {
                    var def = Deferred().resolve(100);
                    return def.promise();
                });
                M.getMax(rChild, column, ['idparent'], filter, expr)
                    .done(function (res) {
                        expect(DAC.readSingleValue).toHaveBeenCalled();
                        expect(res).toBe(100);
                        done();
                    })
                    .fail(function (err) {
                        expect(err).toBeUndefined();
                        done();
                    });
            });
    });

    describe('doPost', function () {
        var DAC,
            env,
            postData,
            d,
            changes;

        beforeEach(function (done) {
            var rowCount, row, i, t,
                d = new DataSet('d');

            i = 11;
            while (--i > 0) {
                t = d.newTable('tab' + i);
                t.columns.push('id' + i);
                t.columns.push('data' + i);
                if (i < 10) {
                    t.columns.push('idExt' + (i + 1));
                    d.newRelation('r' + i + 'a', 'tab' + i, ['idExt' + (i + 1)], 'tab' + (i + 1), ['id' + (i + 1)]);
                }
                if (i < 9) {
                    t.columns.push('idExt' + (i + 2));
                }
                if (i < 8) {
                    t.columns.push('idExt' + (i + 3));
                }
                rowCount = 0;
                while (++rowCount < 6) {
                    row = t.newRow({
                        'id': rowCount,
                        'data': 'about' + rowCount,
                        'idExt': 2 * rowCount,
                        'tabRif': t.name
                    });
                }
            }


            DAC = undefined;
            env = new Environment();

            dbList.getDataAccess('test')
                .done(function (conn) {
                    DAC = conn;
                    spyOn(DAC, 'close').andCallThrough();
                    postData = new PostData(DAC, env);
                    changes = postData.changeList(d);
                    done();
                })
                .fail(function (err) {
                    done();
                });
        });

        it('should return an instance of a PostData ', function () {
            expect(postData).toEqual(jasmine.any(PostData));
        });

        it('should return a resolved promise to {checks:[]} when called with no change ', function () {
            postData.doPost([])
                .done(function (res) {
                    expect(res).toEqual({checks: []});
                    expect(DAC.isOpen).toBeFalsy();
                })
                .fail(function (err) {
                    expect(err).toBeUndefined();
                });
        });

        it('should call opt.callChecks when there is something to save', function (done) {
            var opt = {
                getChecks: function () {
                    var def = Deferred(),
                        res = {checks: [], shouldContinue: true};
                    def.resolve(res);
                    return def.promise();
                },
                optimisticLocking: new OptimisticLocking(['lt', 'lu'], ['ct', 'cu', 'lt', 'lu'])
            };
            spyOn(opt, 'getChecks').andCallThrough();
            expect(opt.getChecks).not.toHaveBeenCalled();

            postData.doPost(changes, opt)
                .always(function (res) {
                    expect(opt.getChecks).toHaveBeenCalled();
                    expect(DAC.isOpen).toBeFalsy();
                    done();
                });
        });

        it('should append error got from callChecks to output errors', function (done) {

            var errNum = 12,
                opt = {
                    getChecks: function (post) {
                        var def = Deferred(),
                            res = {checks: [{code: errNum}], shouldContinue: false};
                        errNum += 1;
                        def.resolve(res);
                        //console.log('returning checks:'+res);
                        return def.promise();
                    },
                    optimisticLocking: new OptimisticLocking(['lt', 'lu'], ['ct', 'cu', 'lt', 'lu'])
                };
            spyOn(opt, 'getChecks').andCallThrough();

            postData.doPost(changes, opt)
                .always(function (res) {
                    expect(res.checks).toContain({code: 12});
                    expect(DAC.isOpen).toBeFalsy();
                    done();
                });
        });

        it('should NOT call open and begin transaction if precheck returned shouldContinue is false', function (done) {

            var errNum = 12,
                opt = {
                    getChecks: function (post) {
                        var def = Deferred(),
                            res = {
                                checks: [
                                    {code: errNum}
                                ], shouldContinue: false
                            };
                        errNum += 1;
                        def.resolve(res);
                        return def.promise();
                    },
                    optimisticLocking: new OptimisticLocking(['lt', 'lu'], ['ct', 'cu', 'lt', 'lu'])
                };
            spyOn(DAC, 'open').andCallThrough();
            spyOn(DAC, 'beginTransaction').andCallThrough();

            postData.doPost(changes, opt)
                .always(function (res) {
                    expect(DAC.open).not.toHaveBeenCalled();
                    expect(DAC.beginTransaction).not.toHaveBeenCalled();
                    expect(DAC.isOpen).toBeFalsy();
                    done();
                });
        });


        it('should call open and beginTransaction if prechecks returned shouldContinue is true', function (done) {

            var errNum = 12,
                opt = {
                    getChecks: function (post) {
                        var def = Deferred(),
                            res = {
                                checks: [
                                    {code: errNum}
                                ], shouldContinue: true
                            };
                        errNum += 1;
                        def.resolve(res);
                        return def.promise();
                    },
                    optimisticLocking: new OptimisticLocking(['lt', 'lu'], ['ct', 'cu', 'lt', 'lu'])
                };
            spyOn(DAC, 'open').andCallThrough();
            spyOn(DAC, 'beginTransaction').andCallThrough();

            postData.doPost(changes, opt)
                .fail(function (err) {
                    expect(err).toBeUndefined();
                })
                .always(function (res) {
                    expect(DAC.open).toHaveBeenCalled();
                    expect(DAC.beginTransaction).toHaveBeenCalled();
                    expect(DAC.close).toHaveBeenCalled();
                    expect(DAC.isOpen).toBeFalsy();
                    done();
                });
        }, 5000);

        it('should call physicalPostBatch if prechecks returned shouldContinue is true', function (done) {

            var errNum = 12,
                opt = {
                    getChecks: function (post) {
                        var def = Deferred(),
                            res = {
                                checks: [
                                    {code: errNum, post: post}
                                ], shouldContinue: true
                            };
                        errNum += 1;
                        def.resolve(res);
                        return def.promise();
                    },
                    optimisticLocking: new OptimisticLocking(['lt', 'lu'], ['ct', 'cu', 'lt', 'lu'])
                };

            spyOn(postData, 'physicalPostBatch').andCallFake(function () {
                var def = Deferred();
                def.resolve();
                return def.promise();
            });

            postData.doPost(changes, opt)
                .fail(function (err) {
                    expect(err).toBeUndefined();
                })
                .always(function (res) {
                    expect(postData.physicalPostBatch).toHaveBeenCalled();
                    expect(DAC.close).toHaveBeenCalled();
                    expect(DAC.isOpen).toBeFalsy();
                    done();
                });
        }, 5000);

        it('should call opt.log if physicalPostBatch is resolved and opt.log is given', function (done) {

            var errNum = 12,
                opt = {
                    getChecks: function (post) {
                        var def = Deferred(),
                            res = {
                                checks: [
                                    {code: errNum, post: post}
                                ], shouldContinue: true
                            };
                        errNum += 1;
                        def.resolve(res);
                        return def.promise();
                    },
                    optimisticLocking: new OptimisticLocking(['lt', 'lu'], ['ct', 'cu', 'lt', 'lu']),
                    log: function (conn, changes) {
                        var def = Deferred();
                        def.resolve();
                        return def.promise();
                    }
                };

            spyOn(postData, 'physicalPostBatch').andCallFake(function () {
                var def = Deferred();
                def.resolve();
                return def.promise();
            });

            spyOn(opt, 'log').andCallThrough();

            postData.doPost(changes, opt)
                .fail(function (err) {
                    expect(err).toBeUndefined();
                })
                .always(function (res) {
                    expect(opt.log.callCount).toBe(1);
                    expect(DAC.close).toHaveBeenCalled();
                    expect(DAC.isOpen).toBeFalsy();
                    done();
                });
        });

        it('should not call getChecks(post) if given opt.log reject his promise', function (done) {

            var errNum = 12,
                opt = {
                    getChecks: function (post) {
                        var def = Deferred(),
                            res = {
                                checks: [
                                    {code: errNum, post: post}
                                ], shouldContinue: true
                            };
                        errNum += 1;
                        def.resolve(res);
                        return def.promise();
                    },
                    optimisticLocking: new OptimisticLocking(['lt', 'lu'], ['ct', 'cu', 'lt', 'lu']),
                    log: function (conn, changes) {
                        var def = Deferred();
                        def.reject('log reject');
                        return def.promise();
                    }
                };

            spyOn(postData, 'physicalPostBatch').andCallFake(function () {
                var def = Deferred();
                def.resolve();
                return def.promise();
            });
            spyOn(opt, 'getChecks').andCallThrough();
            spyOn(opt, 'log').andCallThrough();

            postData.doPost(changes, opt)
                .fail(function (err) {
                    expect(err).toBeUndefined();
                })
                .always(function (res) {
                    expect(opt.getChecks.callCount).toBe(1);
                    expect(opt.log.callCount).toBe(1);
                    expect(res.checks).toContain({msg: 'log reject'});
                    expect(DAC.close).toHaveBeenCalled();
                    expect(DAC.isOpen).toBeFalsy();
                    done();
                });
        });

        it('should call postChecks if physicalPostBatch is resolved', function (done) {

            var errNum = 12,
                opt = {
                    getChecks: function (post) {
                        var def = Deferred(),
                            res = {
                                checks: [
                                    {code: errNum, post: post}
                                ], shouldContinue: true
                            };
                        errNum += 1;
                        def.resolve(res);
                        return def.promise();
                    },
                    optimisticLocking: new OptimisticLocking(['lt', 'lu'], ['ct', 'cu', 'lt', 'lu'])
                };

            spyOn(postData, 'physicalPostBatch').andCallFake(function () {
                var def = Deferred();
                def.resolve();
                return def.promise();
            });

            spyOn(opt, 'getChecks').andCallThrough();

            postData.doPost(changes, opt)
                .fail(function (err) {
                    expect(err).toBeUndefined();
                })
                .always(function (res) {
                    //console.log(res);
                    expect(opt.getChecks.callCount).toBe(2);
                    expect(res.checks).toContain({code: 12, post: false});
                    expect(res.checks).toContain({code: 13, post: true});
                    expect(DAC.close).toHaveBeenCalled();
                    expect(DAC.isOpen).toBeFalsy();
                    done();
                });
        });

        it('should NOT call postChecks if physicalPostBatch is rejected', function (done) {

            var errNum = 12,
                opt = {
                    getChecks: function (post) {
                        var def = Deferred(),
                            res = {
                                checks: [
                                    {code: errNum, post: post}
                                ], shouldContinue: true
                            };
                        errNum += 1;
                        def.resolve(res);
                        return def.promise();
                    },
                    optimisticLocking: new OptimisticLocking(['lt', 'lu'], ['ct', 'cu', 'lt', 'lu'])
                };

            spyOn(postData, 'physicalPostBatch').andCallFake(function () {
                var def = Deferred();
                def.reject('physicalPostBatch fake error');
                return def.promise();
            });

            spyOn(opt, 'getChecks').andCallThrough();

            postData.doPost(changes, opt)
                .fail(function (err) {
                    expect(err).toBeUndefined();
                })
                .always(function (res) {
                    //console.log(res);
                    expect(opt.getChecks.callCount).toBe(1);
                    expect(res.checks).toContain({code: 12, post: false});
                    expect(res.checks).not.toContain({code: 13, post: true});
                    expect(res.checks).toContain({msg: 'physicalPostBatch fake error'});
                    expect(DAC.close).toHaveBeenCalled();
                    expect(DAC.isOpen).toBeFalsy();
                    done();
                });
        });


        it('should NOT call opt.doUpdate if physicalPostBatch is resolved and opt.doUpdate is given and there are checks',
            function (done) {

                var errNum = 12,
                    opt = {
                        getChecks: function (post) {
                            var def = Deferred(),
                                res = {
                                    checks: [
                                        {code: errNum, post: post}
                                    ], shouldContinue: true
                                };
                            errNum += 1;
                            def.resolve(res);
                            return def.promise();
                        },
                        optimisticLocking: new OptimisticLocking(['lt', 'lu'], ['ct', 'cu', 'lt', 'lu']),
                        doUpdate: function () {
                            var def = Deferred();
                            def.resolve();
                            return def.promise();
                        }
                    };

                spyOn(postData, 'physicalPostBatch').andCallFake(function () {
                    var def = Deferred();
                    def.resolve();
                    return def.promise();
                });
                spyOn(opt, 'doUpdate').andCallThrough();

                postData.doPost(changes, opt)
                    .fail(function (err) {
                        expect(err).toBeUndefined();
                    })
                    .always(function (res) {
                        //console.log(res);
                        expect(opt.doUpdate).not.toHaveBeenCalled();
                        expect(DAC.close).toHaveBeenCalled();
                        expect(DAC.isOpen).toBeFalsy();
                        done();
                    });
            });

        it('should  call opt.doUpdate if physicalPostBatch is resolved and opt.doUpdate is given and there are no checks',
            function (done) {

                var errNum = 12,
                    opt = {
                        getChecks: function (post) {
                            var def = Deferred(),
                                res = {
                                    checks: [], //[{code: errNum, post: post}],
                                    shouldContinue: true
                                };
                            errNum += 1;
                            def.resolve(res);
                            return def.promise();
                        },
                        optimisticLocking: new OptimisticLocking(['lt', 'lu'], ['ct', 'cu', 'lt', 'lu']),
                        doUpdate: function () {
                            var def = Deferred();
                            def.resolve({checks: []});
                            return def.promise();
                        }
                    };

                spyOn(postData, 'physicalPostBatch').andCallFake(function () {
                    var def = Deferred();
                    def.resolve();
                    return def.promise();
                });
                spyOn(opt, 'doUpdate').andCallThrough();

                postData.doPost(changes, opt)
                    .fail(function (err) {
                        expect(err).toBeUndefined();
                    })
                    .always(function (res) {
                        //console.log(res);
                        expect(opt.doUpdate).toHaveBeenCalled();
                        expect(DAC.close).toHaveBeenCalled();
                        expect(DAC.isOpen).toBeFalsy();
                        done();
                    });
            });


        it('should  call commit if optional doUpdate resolves',
            function (done) {

                var errNum = 12,
                    opt = {
                        getChecks: function (post) {
                            var def = Deferred(),
                                res = {
                                    checks: [], //[{code: errNum, post: post}],
                                    shouldContinue: true
                                };
                            errNum += 1;
                            def.resolve(res);
                            return def.promise();
                        },
                        optimisticLocking: new OptimisticLocking(['lt', 'lu'], ['ct', 'cu', 'lt', 'lu']),
                        doUpdate: function () {
                            var def = Deferred();
                            def.resolve({checks: []});
                            return def.promise();
                        }
                    };

                spyOn(postData, 'physicalPostBatch').andCallFake(function () {
                    var def = Deferred();
                    def.resolve();
                    return def.promise();
                });
                spyOn(opt, 'doUpdate').andCallThrough();
                spyOn(DAC, 'commit').andCallThrough();

                postData.doPost(changes, opt)
                    .fail(function (err) {
                        expect(err).toBeUndefined();
                    })
                    .always(function () {
                        //console.log(res);
                        expect(DAC.commit).toHaveBeenCalled();
                        expect(DAC.close).toHaveBeenCalled();
                        expect(DAC.isOpen).toBeFalsy();
                        done();
                    });
            });

        it('should  call rollBack (and not commit) if optional doUpdate is rejected',
            function (done) {

                var errNum = 12,
                    opt = {
                        getChecks: function (post) {
                            var def = Deferred(),
                                res = {
                                    checks: [], //[{code: errNum, post: post}],
                                    shouldContinue: true
                                };
                            errNum += 1;
                            def.resolve(res);
                            return def.promise();
                        },
                        optimisticLocking: new OptimisticLocking(['lt', 'lu'], ['ct', 'cu', 'lt', 'lu']),
                        doUpdate: function () {
                            var def = Deferred();
                            def.reject('another doUpdate fake error');
                            return def.promise();
                        }
                    };

                spyOn(postData, 'physicalPostBatch').andCallFake(function () {
                    var def = Deferred();
                    def.resolve();
                    return def.promise();
                });
                spyOn(opt, 'doUpdate').andCallThrough();
                spyOn(DAC, 'commit').andCallThrough();
                spyOn(DAC, 'rollback').andCallThrough();

                postData.doPost(changes, opt)
                    .fail(function (err) {
                        expect(err).toBeUndefined();
                    })
                    .always(function (res) {
                        //console.log(res);
                        expect(DAC.commit).not.toHaveBeenCalled();
                        expect(DAC.rollback).toHaveBeenCalled();
                        expect(res.checks).toContain({msg: 'another doUpdate fake error'});
                        expect(DAC.close).toHaveBeenCalled();
                        expect(DAC.isOpen).toBeFalsy();
                        done();
                    });
            });

        it('should  call rollBack (and not commit) if physicalPostBatch is rejected',
            function (done) {

                var errNum = 12,
                    opt = {
                        getChecks: function (post) {
                            var def = Deferred(),
                                res = {
                                    checks: [], //[{code: errNum, post: post}],
                                    shouldContinue: true
                                };
                            errNum += 1;
                            def.resolve(res);
                            return def.promise();
                        },
                        optimisticLocking: new OptimisticLocking(['lt', 'lu'], ['ct', 'cu', 'lt', 'lu']),
                        doUpdate: function () {
                            var def = Deferred();
                            def.resolve();
                            return def.promise();
                        }
                    };

                spyOn(postData, 'physicalPostBatch').andCallFake(function () {
                    var def = Deferred();
                    def.reject('physicalPostBatch fake error');
                    return def.promise();
                });
                spyOn(opt, 'doUpdate').andCallThrough();
                spyOn(DAC, 'commit').andCallThrough();
                spyOn(DAC, 'rollback').andCallThrough();

                postData.doPost(changes, opt)
                    .fail(function (err) {
                        expect(err).toBeUndefined();
                    })
                    .always(function (res) {
                        //console.log(res);
                        expect(opt.doUpdate).not.toHaveBeenCalled();
                        expect(DAC.commit).not.toHaveBeenCalled();
                        expect(DAC.rollback).toHaveBeenCalled();
                        expect(res.checks).toContain({msg: 'physicalPostBatch fake error'});
                        expect(DAC.close).toHaveBeenCalled();
                        expect(DAC.isOpen).toBeFalsy();
                        done();
                    });
            });
    });

    describe('getSqlStatements', function () {
        var DAC,
            env,
            conn,
            postData,
            d,
            optimisticLocking,
            changes;

        beforeEach(function (done) {
            var rowCount, row, i, t,
                d = new DataSet('d');

            i = 11;
            while (--i > 0) {
                t = d.newTable('tab' + i);
                t.columns.push('id' + i);
                t.columns.push('data' + i);
                if (i < 10) {
                    t.columns.push('idExt' + (i + 1));
                    d.newRelation('r' + i + 'a', 'tab' + i, ['idExt' + (i + 1)], 'tab' + (i + 1), ['id' + (i + 1)]);
                }
                if (i < 9) {
                    t.columns.push('idExt' + (i + 2));
                }
                if (i < 8) {
                    t.columns.push('idExt' + (i + 3));
                }
                rowCount = 0;
                while (++rowCount < 6) {
                    row = t.newRow({
                        'id': rowCount,
                        'data': 'about' + rowCount,
                        'idExt': 2 * rowCount,
                        'tabRif': t.name
                    });
                }
            }
            optimisticLocking = new OptimisticLocking(['lt', 'lu'], ['ct', 'cu', 'lt', 'lu']);

            DAC = undefined;
            env = new Environment();

            dbList.getDataAccess('test')
                .done(function (DA) {
                    DAC = DA;
                    conn = DA.sqlConn;
                    postData = new PostData(DAC, env);
                    changes = postData.changeList(d);
                    done();
                })
                .fail(function (err) {
                    done();
                });
        });

        it('should be a function', function () {
            expect(postData.getSqlStatements).toEqual(jasmine.any(Function));
            expect(postData.sqlConn).toBe(conn);
        });

        it('should call calcAllAutoId for every given row', function (done) {
            spyOn(postData, 'calcAllAutoId').andCallFake(function (r) {
                var def = Deferred();
                def.resolve(false);
                return def.promise();
            });
            spyOn(DAC, 'runCmd').andCallFake(function () {
                var def = Deferred();
                def.resolve(-1);
                return def.promise();
            });
            postData.getSqlStatements(changes, optimisticLocking)
                .done(function (res) {
                    expect(postData.calcAllAutoId).toHaveBeenCalled();
                    expect(postData.calcAllAutoId.callCount).toBe(changes.length);
                    done();
                });
        });

        it('should call calcAllAutoId one at a time', function (done) {
            var overlap = false, isIn = false;
            spyOn(postData, 'calcAllAutoId').andCallFake(function (r) {
                if (isIn) {
                    overlap = true;
                }
                isIn = true;
                var def = Deferred();
                process.nextTick(function () {
                    isIn = false;
                    def.resolve(false);
                });

                return def.promise();
            });
            spyOn(DAC, 'runCmd').andCallFake(function () {
                var def = Deferred();
                def.resolve(-1);
                return def.promise();
            });
            postData.getSqlStatements(changes, optimisticLocking)
                .done(function (res) {
                    expect(postData.calcAllAutoId).toHaveBeenCalled();
                    expect(postData.calcAllAutoId.callCount).toBe(changes.length);
                    expect(overlap).toBeFalsy();
                    done();
                });
        });

        it('if calcAllAutoId one does fail, should reject request and stop', function (done) {
            var overlap = false, isIn = false, nRows = 0;
            spyOn(postData, 'calcAllAutoId').andCallFake(function (r) {
                if (isIn) {
                    overlap = true;
                }
                var def = Deferred();
                if (nRows === 5) {
                    def.reject('some reasons');
                    return def.promise();
                }
                isIn = true;
                process.nextTick(function () {
                    isIn = false;
                    nRows += 1;
                    def.resolve(false);
                });

                return def.promise();
            });
            spyOn(DAC, 'runCmd').andCallFake(function () {
                var def = Deferred();
                def.resolve(-1);
                return def.promise();
            });
            postData.getSqlStatements(changes, optimisticLocking)
                .done(function (res) {
                    expect(true).toBeFalsy();
                    done();
                })
                .fail(function (err) {
                    expect(postData.calcAllAutoId).toHaveBeenCalled();
                    expect(postData.calcAllAutoId.callCount).toBe(6);
                    expect(overlap).toBeFalsy();
                    done();
                });
        });


        it('should call DAC.getPostCommand and giveErrorNumberDataWasNotWritten for every given row', function (done) {
            var countCommands = 0;
            spyOn(DAC, 'getPostCommand').andCallFake(function (row, locking, env) {
                var def = Deferred();
                countCommands += 1;
                def.resolve('fake sql' + countCommands);
                return def.promise();
            });
            spyOn(conn, 'giveErrorNumberDataWasNotWritten').andCallThrough();

            spyOn(DAC, 'runCmd').andCallFake(function () {
                var def = Deferred();
                def.resolve(-1);
                return def.promise();
            });

            //console.log('from now is the time')
            postData.getSqlStatements(changes, optimisticLocking)
                .done(function (res) {
                    expect(DAC.getPostCommand).toHaveBeenCalled();
                    expect(DAC.getPostCommand.callCount).toBe(changes.length);
                    expect(conn.giveErrorNumberDataWasNotWritten).toHaveBeenCalled();
                    expect(conn.giveErrorNumberDataWasNotWritten.callCount).toBe(changes.length);
                    done();
                });
        });

        it('should call appendCommands for every given row ', function (done) {
            var nRows = 0;
            var countCommands = 0;
            spyOn(DAC, 'getPostCommand').andCallFake(function (row, locking, env) {
                var def = Deferred();
                countCommands += 1;
                var sql = 'fake sql' + countCommands;
                def.resolve(sql);
                return def.promise();
            });
            spyOn(conn, 'appendCommands').andCallThrough();
            spyOn(postData, 'calcAllAutoId').andCallFake(function (r) {
                var def = Deferred();
                process.nextTick(function () {
                    nRows += 1;
                    def.resolve(false);
                });

                return def.promise();
            });

            spyOn(DAC, 'runCmd').andCallFake(function () {
                var def = new Deferred();
                def.resolve(-1);
                return def.promise();
            });
            postData.getSqlStatements(changes, optimisticLocking)
                .done(function (res) {
                    expect(conn.appendCommands).toHaveBeenCalled();
                    expect(conn.appendCommands.callCount).toBe(changes.length);
                    done();
                })
                .fail(function (err) {
                    expect(err).toBeUndefined();
                    done();
                });
        });

        it('should call appendCommands for every given row  (all single sql commands)', function (done) {
            var nRows = 0;
            var countCommands = 0;
            var expectedSql = [],
                results = [];
            spyOn(DAC, 'getPostCommand').andCallFake(function (row, locking, env) {
                countCommands += 1;
                var sql = 'fake sql' + countCommands;
                expectedSql.push(sql);
                return sql;
            });
            spyOn(conn, 'appendCommands').andCallThrough();
            spyOn(postData, 'calcAllAutoId').andCallFake(function (r) {
                var def = Deferred();
                process.nextTick(function () {
                    nRows += 1;
                    def.resolve(true);
                });

                return def.promise();
            });

            spyOn(DAC, 'runCmd').andCallFake(function () {
                var def = Deferred();
                def.resolve(-1);
                return def.promise();
            });
            postData.getSqlStatements(changes, optimisticLocking)
                .progress(function (rows, sql) {
                    results.push({rows: rows, sql: sql});
                })
                .done(function (res) {
                    expect(conn.appendCommands).toHaveBeenCalled();
                    expect(conn.appendCommands.callCount).toBe(changes.length);
                    expect(results.length).toBeGreaterThan(0);
                    _.forEach(expectedSql, function (sql) {
                        expect(_.find(results, function (el) {
                            //console.log(el);
                            return el.sql.indexOf(sql) >= 0;
                        })).toBeDefined();
                    });
                    done();
                })
                .fail(function (err) {
                    expect(err).toBeUndefined();
                    done();
                });
        });

        it('should call appendCommands for every given row (grouped sql commands)', function (done) {
            var nRows = 0;
            var countCommands = 0;
            var expectedSql = [],
                results = [];
            spyOn(DAC, 'getPostCommand').andCallFake(function (row, locking, env) {
                countCommands += 1;
                var sql = 'fake sql' + countCommands;
                expectedSql.push(sql);
                return sql;
            });
            spyOn(conn, 'appendCommands').andCallThrough();
            spyOn(postData, 'calcAllAutoId').andCallFake(function (r) {
                var def = Deferred();
                process.nextTick(function () {
                    nRows += 1;
                    def.resolve(false);
                });

                return def.promise();
            });

            spyOn(DAC, 'runCmd').andCallFake(function () {
                var def = Deferred();
                def.resolve(-1);
                return def.promise();
            });
            postData.getSqlStatements(changes, optimisticLocking)
                .progress(function (rows, sql) {
                    results.push({rows: rows, sql: sql});
                })
                .done(function (res) {
                    expect(conn.appendCommands).toHaveBeenCalled();
                    expect(conn.appendCommands.callCount).toBe(changes.length);
                    expect(results.length).toBeGreaterThan(1);
                    _.forEach(expectedSql, function (sql) {
                        expect(_.find(results, function (el) {
                            //console.log(el);
                            return el.sql.indexOf(sql) >= 0;
                        })).toBeDefined();
                    });
                    done();
                })
                .fail(function (err) {
                    expect(err).toBeUndefined();
                    done();
                });
        });

    });

    describe('physicalPostBatch', function () {
        var DAC, conn,
            env,
            postData,
            d,
            optimisticLocking,
            changes;

        beforeEach(function (done) {
            var rowCount, row, i, t,
                d = new DataSet('d');

            i = 11;
            while (--i > 0) {
                t = d.newTable('tab' + i);
                t.columns.push('id' + i);
                t.columns.push('data' + i);
                if (i < 10) {
                    t.columns.push('idExt' + (i + 1));
                    d.newRelation('r' + i + 'a', 'tab' + i, ['idExt' + (i + 1)], 'tab' + (i + 1), ['id' + (i + 1)]);
                }
                if (i < 9) {
                    t.columns.push('idExt' + (i + 2));
                }
                if (i < 8) {
                    t.columns.push('idExt' + (i + 3));
                }
                rowCount = 0;
                while (++rowCount < 6) {
                    row = t.newRow({
                        'id': rowCount,
                        'data': 'about' + rowCount,
                        'idExt': 2 * rowCount,
                        'tabRif': t.name
                    });
                }
            }
            optimisticLocking = new OptimisticLocking(['lt', 'lu'], ['ct', 'cu', 'lt', 'lu']);

            DAC = undefined;
            env = new Environment();
            dbList.getDataAccess('test')
                .done(function (c) {
                    DAC = c;
                    conn = c.sqlConn;
                    postData = new PostData(DAC, env);
                    changes = postData.changeList(d);
                    done();
                })
                .fail(function (err) {
                    done();
                });
        });

        it('should be a function', function () {
            expect(postData.physicalPostBatch).toEqual(jasmine.any(Function));
        });

        it('should call getSqlStatements', function (done) {
            spyOn(postData, 'getSqlStatements').andCallThrough();
            spyOn(DAC, 'runCmd').andCallFake(function () {
                var def = Deferred();
                def.resolve(-1);
                return def.promise();
            });
            postData.physicalPostBatch(changes, optimisticLocking)
                .done(function () {
                    expect(postData.getSqlStatements).toHaveBeenCalled();
                    done();
                });
        });

        it('should call runCmd', function (done) {
            spyOn(DAC, 'runCmd').andCallFake(function () {
                var def = Deferred();
                def.resolve(-1);
                return def.promise();
            });
            postData.physicalPostBatch(changes, optimisticLocking)
                .done(function () {
                    expect(DAC.runCmd).toHaveBeenCalled();
                    done();
                });
        });


        it('if runCmd fails then physicalPostBatch should fail too', function (done) {
            var nRows = 0;
            var countCommands = 0;
            var expectedSql = [],
                results = [];
            spyOn(DAC, 'getPostCommand').andCallFake(function (row, locking, env) {
                countCommands += 1;
                var sql = 'fake sql' + countCommands;
                expectedSql.push(sql);
                return sql;
            });
            spyOn(conn, 'appendCommands').andCallThrough();
            spyOn(postData, 'calcAllAutoId').andCallFake(function (r) {
                var def = Deferred();
                process.nextTick(function () {
                    nRows += 1;
                    def.resolve(false);
                });

                return def.promise();
            });

            var nOk = 0;
            spyOn(DAC, 'runCmd').andCallFake(function (sqlComplete) {
                var def = Deferred();
                nOk += 1;
                if (nOk < 50) {
                    def.resolve(-1);
                } else {
                    def.reject('runCmd fake error');
                }

                return def.promise();
            });
            postData.physicalPostBatch(changes, optimisticLocking)
                .done(function (res) {
                    expect(true).toBeFalsy();
                    done();
                })
                .fail(function (err) {
                    expect(err).toContain('runCmd fake error');
                    done();
                });
        });


        it('if runCmd resolves into a bad number, an internal error should be raised', function (done) {
            var nRows = 0;
            var countCommands = 0;
            var expectedSql = [],
                results = [];
            spyOn(DAC, 'getPostCommand').andCallFake(function (row, locking, env) {
                countCommands += 1;
                var sql = 'fake sql' + countCommands;
                expectedSql.push(sql);
                return sql;
            });
            spyOn(conn, 'appendCommands').andCallThrough();
            spyOn(postData, 'calcAllAutoId').andCallFake(function (r) {
                var def = Deferred();
                process.nextTick(function () {
                    nRows += 1;
                    def.resolve(false);
                });

                return def.promise();
            });

            var nOk = 0;
            spyOn(DAC, 'runCmd').andCallFake(function () {
                var def = Deferred();
                nOk += 1;
                if (nOk < 50) {
                    def.resolve(-1);
                } else {
                    def.resolve(99999);
                }

                return def.promise();
            });
            postData.physicalPostBatch(changes, optimisticLocking)
                .done(function () {
                    expect(true).toBeFalsy();
                    done();
                })
                .fail(function (err) {
                    expect(err).toContain('internal error');
                    done();
                });
        });


        it('runCmd should be called with every sql cmd got from getPostCommand (grouped)', function (done) {
            var nRows = 0;
            var countCommands = 0;
            var expectedSql = [],
                runnedSql = [];
            spyOn(DAC, 'getPostCommand').andCallFake(function (row, locking, env) {
                countCommands += 1;
                var sql = 'fake sql >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>' + countCommands;
                expectedSql.push(sql);
                return sql;
            });
            spyOn(conn, 'appendCommands').andCallThrough();
            spyOn(postData, 'calcAllAutoId').andCallFake(function (r) {
                var def = Deferred();
                process.nextTick(function () {
                    nRows += 1;
                    def.resolve(true);
                });

                return def.promise();
            });

            var nOk = 0;
            spyOn(DAC, 'runCmd').andCallFake(function (sqlComplete) {
                var def = Deferred();
                nOk += 1;
                runnedSql.push(sqlComplete);
                def.resolve(-1);
                return def.promise();
            });
            postData.physicalPostBatch(changes, optimisticLocking)
                .done(function () {
                    _.forEach(expectedSql, function (sql) {
                        expect(_.find(runnedSql, function (el) {
                            //console.log(el);
                            return el.indexOf(sql) >= 0;
                        })).toBeDefined();
                    });
                    done();
                })
                .fail(function (err) {
                    expect(err).toBeUndefined();
                    done();
                });
        });

        it('runCmd should be called with every sql cmd got from getPostCommand (single)', function (done) {
            var nRows = 0;
            var countCommands = 0;
            var expectedSql = [],
                runnedSql = [];
            spyOn(DAC, 'getPostCommand').andCallFake(function () {  //row, locking, env
                countCommands += 1;
                var sql = 'fake sql >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>' + countCommands;
                expectedSql.push(sql);
                return sql;
            });
            spyOn(conn, 'appendCommands').andCallThrough();
            spyOn(postData, 'calcAllAutoId').andCallFake(function (r) {
                var def = Deferred();
                process.nextTick(function () {
                    nRows += 1;
                    def.resolve(false);
                });

                return def.promise();
            });

            var nOk = 0;
            spyOn(DAC, 'runCmd').andCallFake(function (sqlComplete) {
                var def = Deferred();
                nOk += 1;
                runnedSql.push(sqlComplete);
                def.resolve(-1);
                return def.promise();
            });
            postData.physicalPostBatch(changes, optimisticLocking)
                .done(function (res) {
                    _.forEach(expectedSql, function (sql) {
                        expect(_.find(runnedSql, function (el) {
                            //console.log(el);
                            return el.indexOf(sql) >= 0;
                        })).toBeDefined();
                    });
                    done();
                })
                .fail(function (err) {
                    expect(err).toBeUndefined();
                    done();
                });
        });

    });


    describe('getSelectAllViews', function () {
        var DAC,
            env,
            conn,
            postData,
            d,
            optimisticLocking,
            changes;

        beforeEach(function (done) {
            var rowCount, row, i, t;
            d = new DataSet('d');

            i = 11;
            while (--i > 0) {
                t = d.newTable('tab' + i);
                t.columns.push('id' + i);
                t.columns.push('data' + i);
                if (i < 10) {
                    t.columns.push('idExt' + (i + 1));
                    d.newRelation('r' + i + 'a', 'tab' + i, ['idExt' + (i + 1)], 'tab' + (i + 1), ['id' + (i + 1)]);
                }
                if (i < 9) {
                    t.columns.push('idExt' + (i + 2));
                }
                if (i < 8) {
                    t.columns.push('idExt' + (i + 3));
                }
                t.key(['id' + i]);
                rowCount = 0;
                while (++rowCount < 6) {
                    row = t.newRow({
                        'id': rowCount,
                        'data': 'about' + rowCount,
                        'idExt': 2 * rowCount,
                        'tabRif': t.name
                    });
                }
            }
            optimisticLocking = new OptimisticLocking(['lt', 'lu'], ['ct', 'cu', 'lt', 'lu']);

            DAC = undefined;
            env = new Environment();
            dbList.getDataAccess('test')
                .done(function (conn) {
                    DAC = conn;
                    postData = new PostData(DAC, env);
                    changes = postData.changeList(d);
                    done();
                })
                .fail(function (err) {
                    done();
                });
        });

        it('should be a function', function () {
            expect(postData.getSelectAllViews).toEqual(jasmine.any(Function));
        });

        it('should return an array', function () {
            expect(postData.getSelectAllViews(changes)).toEqual(jasmine.any(Array));
        });

        it('should return an array of Select', function () {
            d.tables.tab3.tableForWriting('realtable3');
            expect(postData.getSelectAllViews(changes).length).toBeGreaterThan(0);
            expect(postData.getSelectAllViews(changes)[0]).toEqual(jasmine.any(Select));
        });

        it('returned Select should be on view table', function () {
            d.tables.tab3.tableForWriting('realtable3');
            expect(postData.getSelectAllViews(changes)[0].alias).toEqual('tab3');
        });

        it('returned Select should read from view table', function () {
            d.tables.tab3.tableForWriting('realtable3');
            expect(postData.getSelectAllViews(changes)[0].tableName).toEqual('tab3');
        });
    });

});



describe('destroy dataBase', function () {
'use strict';
    var sqlConn;
    beforeEach(function (done) {
        dbList.setDbInfo('test', good);
        sqlConn = dbList.getConnection('test');
        sqlConn.open().
            done(function () {
                done();
            });
    });

    afterEach(function () {
        dbList.delDbInfo('test');
        if (sqlConn) {
            sqlConn.destroy();
        }
        sqlConn = null;
        if (fs.existsSync('test/dbList.bin')) {
            fs.unlinkSync('test/dbList.bin');
        }
    });

    it('should run the destroy script', function (done) {
        sqlConn.run(fs.readFileSync(path.join('test', 'destroy.sql')).toString())
            .done(function () {
                expect(true).toBeTruthy();
                done();
            })
            .fail(function (res) {
                expect(res).toBeUndefined();
                done();
            });
    });
});
