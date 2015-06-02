var request = require('request'),
    async = require('async'),
    _ = require('lodash'),
    safe = require('safe'),
    moment = require('moment'),
    mongo = require('mongodb'),
    cf = require("./config.js");
var cfg = cf();
var dbc = new mongo.Db(
    cfg.mongo.db,
    new mongo.Server(cfg.mongo.host, cfg.mongo.port, cfg.mongo.opts), {native_parser: false, safe:true, maxPoolSize: 100}
);
var db;
stationObj = {};
routesObj = {};
lastPos = {};

function getJsonData(url, cb){
    async.waterfall([
        function(cb){
            request(url, cb);
        },
        function(response, body, cb){
            if (response.statusCode != 200)
                return cb('Wrong status code from server: ' + response.statusCode);
            var routes = JSON.parse(body);
            cb(null, routes);
        }
    ], cb);
}

/*{id, fromSTid, toSTid, type, num}*/
function getRoutes(cb){
    getJsonData("http://bus62.ru/php/getRoutes.php?city=ryazan&info=01234", safe.sure(cb, function(routes){
        if (!_.isArray(routes))
            return cb('Wrong server answer');
        var ret = [];
        _.forEach(routes, function(route){
            var obj = {
                fromSTid: route.fromstid,
                toSTid: route.tostid,
                id: route.id,
                num: route.num
            };
            var type = route.type.toString().toLowerCase();
            if (type == 'а' || type == 'a') obj.type = 'A'; else
            if (type == 'т' || type == 't') obj.type = 'T'; else
            if (type == 'м' || type == 'm') obj.type = 'M'; else obj.type = 'E';
            ret.push(obj);
        });
        cb(null, ret);
    }));
}

/*{id, name, desc, coords}*/
function getStations(cb){
    getJsonData("http://bus62.ru/php/getStations.php?city=ryazan&info=01234", safe.sure(cb, function(stations){
        if (!_.isArray(stations))
            return cb('Wrong server answer');
        var ret = [];
        _.forEach(stations, function(station){
            var obj = {
                id: station.id,
                name: station.name,
                desc: station.descr,
                coords: [station.lat / 1E6, station.lng / 1E6]
            };
            ret.push(obj);
        });
        cb(null, ret);
    }));
}

/*{id, gosNum, coords, routeId, time}*/
function getTransportLocation(routes, cb){
    async.waterfall([
        function(cb){
            var rids = [];
            _.forEach(routes, function(route){
                rids.push(route.id.toString() + '-0')
            });
            getJsonData("http://bus62.ru/php/getVehiclesMarkers.php?lat0=0&lng0=0&lat1=90&lng1=180&curk=0&city=ryazan&info=01234&rids="+rids.join(','), cb);
        },
        function(res, cb){
            if (!res.anims || !_.isArray(res.anims))
                return cb('Wrong server answer');
            var ret = [];
            _.forEach(res.anims, function(bus){
                var obj = {
                    gosNum: bus.gos_num,
                    id: bus.id,
                    coords: [bus.lat / 1E6, bus.lon / 1E6],
                    routeId: bus.rid,
                    time: moment(bus.lasttime, "DD.MM.YYYY HH:mm:ss").toDate()
                };
                ret.push(obj);
            });
            cb(null, ret);
        }
    ], cb);
}

function updateStations(cb){
    console.log('getting stations...');
    getStations(safe.sure(cb, function(stations){
        console.log('updating stations...');
        if (db){
            db.collection("stations", safe.sure(cb, function(stColl){
                async.eachLimit(stations, 20, function(station, cb){
                    async.waterfall([
                        function find(cb){
                            stColl.findOne({id: station.id}, cb);
                        },
                        function update(fnd, cb){
                            if (fnd){
                                stColl.update({_id: fnd._id}, {$set:{name: station.name, desc:station.desc, coords: station.coords}}, safe.sure(cb, function(){
                                    stationObj[station.id] = station;
                                    stationObj[station.id]._id = fnd._id;
                                    cb();
                                }));
                            }else{
                                stColl.insert(station, safe.sure(cb, function(row){
                                    stationObj[station.id] = row[0];
                                    cb();
                                }));
                            }
                        }
                    ], cb);
                }, cb);
            }));
        }
    }));
}

function updateRoutes(cb) {
    console.log('getting routes...');
    getRoutes(safe.sure(cb, function (routes) {
        console.log('updating routes...');
        if (db) {
            db.collection("routes", safe.sure(cb, function (rtColl) {
                async.eachLimit(routes, 20, function (route, cb) {
                    async.waterfall([
                        function find(cb) {
                            rtColl.findOne({id: route.id}, cb);
                        },
                        function update(fnd, cb) {
                            if (fnd) {
                                rtColl.update({_id: fnd._id},
                                    {
                                        $set: {
                                            fromSTid: stationObj[route.fromSTid]._id,
                                            toSTid: stationObj[route.toSTid]._id,
                                            type: route.type,
                                            num: route.num
                                        }
                                    }, safe.sure(cb, function () {
                                        routesObj[route.id] = route;
                                        routesObj[route.id].fromST = stationObj[route.fromSTid];
                                        routesObj[route.id].toSt = stationObj[route.toSTid];
                                        delete routesObj[route.id].fromSTid;
                                        delete routesObj[route.id].toSTid;
                                        routesObj[route.id]._id = fnd._id;
                                        cb();
                                    }));
                            } else {
                                rtColl.insert(route, safe.sure(cb, function (row) {
                                    routesObj[route.id] = row[0];
                                    routesObj[route.id].fromST = stationObj[route.fromSTid];
                                    routesObj[route.id].toSt = stationObj[route.toSTid];
                                    delete routesObj[route.id].fromSTid;
                                    delete routesObj[route.id].toSTid;
                                    routesObj[route.id]._id = row._id;
                                    cb();
                                }));
                            }
                        }
                    ], cb);
                }, cb);
            }));
        }
    }));
}

function updateLocations(cb){
    var toUpdate = [];
    async.waterfall([
        function(cb){
            console.log('getting locations...');
            getTransportLocation(_.toArray(routesObj), cb);
        },
        function(locArr, cb){
            console.log('received '+locArr.length.toString()+' values');
            _.forEach(locArr, function(el){
                if (!lastPos[el.id] || lastPos[el.id].time.getTime() !== el.time.getTime()){
                    el.route = routesObj[el.routeId];
                    el.routeId = el.route._id;
                    toUpdate.push(el);
                    lastPos[el.id] = el;
                }
            });
            if (!db)
                return cb('db connection fails');
            db.collection("locations", cb);
        },
        function(lcCol, cb){
            if (toUpdate.length){
                console.log('updating '+toUpdate.length.toString()+' values');
                async.eachLimit(toUpdate, 20, function(loc, cb){
                    var rt = loc.route;
                    delete loc.route;
                    lcCol.insert(loc, safe.sure(cb, function(){
                        loc.route = rt;
                        delete loc.routeId;
                        cb();
                    }));
                }, cb);
            }else{
                console.log('nothing to update')
                cb();
            }
        }
    ], cb);
}

function updater(){
    updateLocations(function(err){
        if (err)
            console.log(err);
        setTimeout(updater, 5000);
    });
}

async.waterfall([
    function dbConnect(cb){
        console.log('connecting to db...');
        dbc.open(cb);
    },
    function dbAuth(_db, cb){
        db = _db;
        console.log('authenticating...');
        dbc.authenticate(cfg.mongo.user, cfg.mongo.password, safe.sure(cb, function(){cb()}));
    },
    function (cb){
        updateStations(cb);
    },
    function (cb){
        updateRoutes(cb);
    }
], function(err){
    if (err)
        console.log(err);
    updater();
});


