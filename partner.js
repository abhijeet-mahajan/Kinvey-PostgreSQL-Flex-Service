var sdk=require('kinvey-flex-sdk');
var pg = require('pg');
var moment = require("moment");

var statuscodes = {
    ok: 200,
    created: 201,
    accepted: 202,
    notFound: 404,
    badRequest: 400,
    unauthorized: 401,
    forbidden: 403,
    notAllowed: 405,
    notImplemented: 501,
    runtimeError: 550
}

sdk.service(function(err, flex) {
    var data = flex.data;
    var partner = data.serviceObject('partner');
    var entityId;
    var outputObject;

    function handleResponse(responsecode, bodyOrDebug, complete){
        switch(responsecode) {
            case 201:
                return complete(bodyOrDebug).created().next();
                break;
            case 200:
                return complete(bodyOrDebug).ok().next();
                break;
            case 404:
                return complete(bodyOrDebug).notFound().next();
                break;
            case 400:
                return complete(bodyOrDebug).badRequest().next();
                break;
            case 401:
                return complete(bodyOrDebug).unauthorized().next();
                break;
            case 403:
                return complete(bodyOrDebug).forbidden().next();
                break;
            case 405:
                return complete(bodyOrDebug).notAllowed().next();
                break;
            case 501:
                return complete(bodyOrDebug).notImplemented().next();
                break;
            case 550:
            default:
                return complete(bodyOrDebug).runtimeError().next();
        }
    }

    function parseQuery(query) {
        var postgreSQLQuery = "";
        var filters = JSON.parse(query.query);
        filters = convertQueryKinveyToPostgreSQL(filters);
        for (var filter in filters) {
            var filtervalue = filters[filter];
            if (typeof filtervalue === 'string' || typeof filtervalue === 'number') {
                if (postgreSQLQuery === "") {
                    postgreSQLQuery += " WHERE ";
                } else {
                    postgreSQLQuery += " AND ";
                }
                postgreSQLQuery += filter + " = '" + filtervalue + "'";
            }
        }

        delete query.query;
        if (query.limit) {
            postgreSQLQuery += " LIMIT " + query.limit;
        }
        if (query.skip) {
            postgreSQLQuery += " OFFSET " + query.skip;
        }
        return postgreSQLQuery;
    }

    function convertEntityPostgreSQLToKinvey(row) {
        row._id = row.id;
        delete row.id;
        row._kmd = {
            ect: row.created_time,
            lmt: row.last_modified_time
        }
        delete row.created_time;
        delete row.last_modified_time;
        row._acl = {};
        return row;
    }

    function convertEntityKinveyToPostgreSQL(row) {
        row.id = row._id;
        delete row._id;
        if (row._kmd){
            if(!row._kmd.ect) {
                row._kmd.ect = moment().toISOString();
            }
            if(!row._kmd.lmt) {
                row._kmd.lmt = moment().toISOString();
            }
        } else {
            row._kmd = {
                ect : moment().toISOString(),
                lmt : moment().toISOString()
            };
        }
        row.created_time = row._kmd.ect;
        row.last_modified_time = row._kmd.lmt;
        delete row._kmd;
        delete row._acl;
        return row;
    }

    function convertQueryKinveyToPostgreSQL(query) {
        if (query._id) {
            query.id = query._id;
            delete query._id;
        }
        if (query["_kmd.ect"]) {
            query.created_time = query["_kmd.ect"];
            delete query["_kmd.ect"];
        }
        if (query["_kmd.lmt"]) {
            query.last_modified_time = query["_kmd.lmt"];
            delete query["_kmd.lmt"];
        }
        return query;
    }

    function constructPostgreSQLquery(method, req) {
        return new Promise(function (fulfill, reject){
            var query = "";
            switch(method) {
                case "onInsert":
                    var body = convertEntityKinveyToPostgreSQL(req.body);
                    query = 'INSERT into partner ';
                    query = formatPostgreSQLInsertQuery(query, body);
                    entityId=body.id;
                    break;
                case "onUpdate":
                    var body = convertEntityKinveyToPostgreSQL(req.body);
                    query = "UPDATE partner SET ";
                    query = formatPostgreSQLUpdateQuery(query, body);
                    query +="where id = '"+entityId+"';"
                    break;
                case "onDeleteById":
                    query = "DELETE FROM partner WHERE id = '" + entityId + "';";
                    break;
                case "onDeleteByQuery":
                    query = "DELETE FROM partner " + parseQuery(req.query) + ";";
                    break;
                case "onGetCount":
                    query = "SELECT COUNT(*) AS count FROM partner;";
                    break;
                case "onGetCountByQuery":
                    query = "SELECT COUNT(*) AS count FROM partner " + parseQuery(req.query) + ";";
                    break;
                case "onGetById":
                    query = "SELECT * FROM partner WHERE id= '" + req.entityId + "';";
                    break;
                case "onGetByQuery":
                    query = "select * from partner " + parseQuery(req.query) + ";";
                    break;
                case "onGetAll":
                default:
                    query+= "SELECT * FROM partner;";
            }
            console.log("query : "+query);
            fulfill(query);
        });
    }

    function formatPostgreSQLInsertQuery(queryText,body){
    var coloumns = [];
    var values = [];

    for (var key in body)
    {
        if (body.hasOwnProperty(key)) {
            coloumns.push(key)
            values.push('\''+body[key]+'\'');
        }
    };
    var keys = coloumns.join(',');
    var keyValues= values.join(',');

    var finalQuery=queryText +'(' +keys + ') '+ 'values' +' (' +keyValues + ');';
    return finalQuery;
}

    function formatPostgreSQLUpdateQuery(queryText,body){
        var coloumns = [];
        var values = [];

        for (var key in body)
        {
            if (body.hasOwnProperty(key))
            {
                queryText += key +' = \''+body[key]+'\' ,'
            }
        };

        queryText = queryText.slice(0, -1);
        return queryText;
    }

    function processPostgreSQLSingleOutput(rows){
        return new Promise(function (fulfill, reject){
            rows.forEach(function(row) {
                row = convertEntityPostgreSQLToKinvey(row);
            });
            fulfill(rows);
        });
    }

    function processPostgreSQLMultipleOutput(rows) {
        return new Promise(function (fulfill, reject){
            fulfill(convertEntityPostgreSQLToKinvey(rows[0]));
        });
    }

    function processPostgreSQLPostInsert(){
        return new Promise(function (fulfill, reject){
            var query = "SELECT * FROM partner WHERE id='" + entityId + "';";
            fulfill(query);
        });
    }

    function processPostgreSQLDeleteOuput(rows) {
        return new Promise(function (fulfill, reject){
            console.log("Row Count : "+outputObject.rowCount);
            fulfill({ count : outputObject.rowCount});
        });
    }

    function processPostgreSQLCountOutput(rows) {
        return new Promise(function (fulfill, reject){
            fulfill({ count : rows[0].count});
        });
    }

    function establishPostgreSQLConnectionAndExecuteQuery(query) {
        return new Promise(function (fulfill, reject)
        {
            //aws
            // var connection = new pg.Client({
            //     user: "",
            //     password: "",
            //     database: "",
            //     port: 5432,
            //     host: "",
            //     ssl: 
            // });

            // local
            var connectionString = "postgres://postgres:kinvey@localhost:5432/partner";
            var connection =new pg.Client(connectionString);

            connection.connect(function(err) {
                if (err) {
                    console.error('error connecting: ' + err.stack);
                    reject(err);
                } else {
                    var finalQuery=connection.query(query);
                    finalQuery.on("row", function (row, result) {
                        result.addRow(row);
                    });

                    finalQuery.on("end", function (result) {
                        outputObject=result;
                        fulfill(result.rows);
                    });
                }
            });
        });
    }

    partner.onGetAll(function(req, complete){
        console.log("inside onGetAll");
        constructPostgreSQLquery("onGetAll", req)
            .then(establishPostgreSQLConnectionAndExecuteQuery)
            .then(processPostgreSQLSingleOutput)
            .then(function (result) {
                handleResponse(statuscodes.ok, result, complete);
            }).catch(function (err) {
            handleResponse(statuscodes.runtimeError, err.stack, complete);
        });
    });

    partner.onGetById(function(req, complete){
        console.log("inside onGetById");
        constructPostgreSQLquery("onGetById", req)
            .then(establishPostgreSQLConnectionAndExecuteQuery)
            .then(processPostgreSQLMultipleOutput)
            .then(function(result){
                handleResponse(statuscodes.ok, result, complete);
            }).catch(function(err){
            handleResponse(statuscodes.runtimeError, err.stack, complete);
        });
    });

    partner.onGetCount(function(req, complete){
        console.log("inside onGetCount");
        constructPostgreSQLquery("onGetCount", req)
            .then(establishPostgreSQLConnectionAndExecuteQuery)
            .then(processPostgreSQLCountOutput)
            .then(function(result){
                handleResponse(statuscodes.ok, result, complete);
            }).catch(function(err){
            handleResponse(statuscodes.runtimeError, err.stack, complete);
        });
    });

    partner.onGetByQuery(function(req, complete){
        console.log("inside onGetByQuery");
        constructPostgreSQLquery("onGetByQuery", req)
            .then(establishPostgreSQLConnectionAndExecuteQuery)
            .then(processPostgreSQLSingleOutput)
            .then(function(result){
                handleResponse(statuscodes.ok, result, complete);
            }).catch(function(err){
            handleResponse(statuscodes.runtimeError, err.stack, complete);
        });
    });

    partner.onGetCountByQuery(function(req, complete){
        console.log("inside onGetCountByQuery");
        constructPostgreSQLquery("onGetCountByQuery", req)
            .then(establishPostgreSQLConnectionAndExecuteQuery)
            .then(processPostgreSQLCountOutput)
            .then(function(result){
                handleResponse(statuscodes.ok, result, complete);
            }).catch(function(err){
            handleResponse(statuscodes.runtimeError, err.stack, complete);
        });
    });

    partner.onInsert(function(req, complete) {
        console.log("inside onInsert");
        constructPostgreSQLquery("onInsert", req)
            .then(establishPostgreSQLConnectionAndExecuteQuery)
             .then(processPostgreSQLPostInsert)
             .then(establishPostgreSQLConnectionAndExecuteQuery)
            .then(processPostgreSQLMultipleOutput)
            .then(function(result){
                handleResponse(statuscodes.created, result, complete);
            }).catch(function(err){
            handleResponse(statuscodes.runtimeError, err.stack, complete);
        });
    });

    partner.onUpdate(function(req, complete){
        console.log("inside onUpdate");
        entityId = req.entityId;

        constructPostgreSQLquery("onUpdate", req)
            .then(establishPostgreSQLConnectionAndExecuteQuery)
            .then(function(rows){
                return processPostgreSQLPostInsert();
            })
            .then(establishPostgreSQLConnectionAndExecuteQuery)
            .then(processPostgreSQLMultipleOutput)
            .then(function(result){
                handleResponse(statuscodes.ok, result, complete);
            }).catch(function(err){
            handleResponse(statuscodes.runtimeError, err.stack, complete);
        });
    });

    partner.onDeleteById(function(req, complete){
        entityId=req.entityId;
        console.log("inside onDeleteById. Deleting.."+ entityId);
        constructPostgreSQLquery("onDeleteById", req)
            .then(establishPostgreSQLConnectionAndExecuteQuery)
            .then(processPostgreSQLDeleteOuput)
            .then(function(result){
                handleResponse(statuscodes.ok, result, complete);
            }).catch(function(err){
            handleResponse(statuscodes.runtimeError, err.stack, complete);
        });
    });

    partner.onDeleteByQuery(function(req, complete){
        console.log("inside onDeleteByQuery");
        constructPostgreSQLquery("onDeleteByQuery", req)
            .then(establishPostgreSQLConnectionAndExecuteQuery)
            .then(processPostgreSQLDeleteOuput)
            .then(function(result){
                handleResponse(statuscodes.ok, result, complete);
            }).catch(function(err){
            handleResponse(statuscodes.runtimeError, err.stack, complete);
        });
    });

});

