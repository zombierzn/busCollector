var request = require('request'),
    async = require('async'),
    _ = require('lodash'),
    safe = require('safe'),
    moment = require('moment'),
    mongo = require('mongodb'),
    cf = require("./config.js");
var jar = request.jar();
var cfg = cf();
var dbc = new mongo.Db(
    cfg.mongo.db,
    new mongo.Server(cfg.mongo.host, cfg.mongo.port, cfg.mongo.opts), {native_parser: false, safe:true, maxPoolSize: 100}
);
var db;
var rzdSess = {};
function getJsonData(url, cb){
    async.waterfall([
        function(cb){
            request({uri: url, jar: jar}, cb);
        },
        function(response, body, cb){
            if (response.statusCode != 200)
                return cb('Wrong status code from server: ' + response.statusCode);
            var data = JSON.parse(body);
            cb(null, data);

        }
    ], cb);
}


function getStationId(station, cb){
    async.waterfall([
        function(cb){
            getJsonData('http://pass.rzd.ru/suggester?lang=ru&stationNamePart=' + encodeURIComponent(station.toUpperCase()), cb);
        },
        function(res, cb){
            if (!res || !_.isArray(res))
                return cb('Wrong server answer');
            var city = _.find(res, function(el){return el.n == station.toUpperCase()});
            cb(null, city);
        }
    ], cb);
}
var trCnt = 8;
function tryAgain(_url, opts, cb){
    console.log('attempt #'+(trCnt-opts.tr+1).toString());
    setTimeout(function(){
        var url = _url;
        if (opts.append && opts.append.sess){
            url += '&rid=' + rzdSess.rid+ '&SESSION_ID=' + rzdSess.SESSION_ID;
        }
        getJsonData(url, function(err, res){
            if (opts.tr == 3){
                console.log('Too much wrong answers, sleep 10s...');
                setTimeout(function(){
                    reaction()
                }, 10000);
            }else if (opts.tr <=0 )
                return cb('no correct answer after '+trCnt.toString()+'attempts');
            else
                reaction();
            function reaction(){
                if (res && res.result == 'RID'){
                    console.log('Getted new RID')
                    rzdSess = {rid: res.rid, SESSION_ID: res.SESSION_ID};
                }
                if (err || !res || res.result != opts.result){
                    console.log('something wrong...')
                    if (res && (res.needCaptcha || res.message=='Произошла внутренняя ошибка.' )) {
                        console.log('captcha request, reseting coockie..')
                        jar = request.jar();
                    }
                    if (res && res.sessExpired){
                        console.log('session expired, try to get new');
                        tryAgain(_url, {tr: opts.tr - 1, result: 'RID'}, safe.sure(cb, function(){
                            var newOpts = JSON.parse(JSON.stringify(opts));
                            newOpts.tr-=2;
                            console.log('trying to get with new RID');
                            tryAgain(_url, newOpts, cb);
                        }));
                    }else{
                        var newOpts = JSON.parse(JSON.stringify(opts));
                        newOpts.tr--;
                        console.log('trying to get again');
                        tryAgain(_url, newOpts, cb);
                    }
                }else{
                    console.log('seems what result is ok');
                    cb(null, res);
                }
            }
        });
    }, 1000);
}

function getTrainTimetable(from, to, date, cb){

    var url = 'http://pass.rzd.ru/timetable/public/ru?STRUCTURE_ID=735&layer_id=5371&dir=0&tfl=3&checkSeats=1&st0='+encodeURIComponent(from.n)+'&code0='+from.c+'&dt0='+moment(date).format('DD.MM.YYYY')+'&st1='+encodeURIComponent(to.n)+'&code1='+to.c+'&dt1='+moment(date).format('DD.MM.YYYY');
    async.waterfall([
        function(cb){
            console.log('trying to get RID...');
            tryAgain(url, {tr: trCnt, result: 'RID'}, cb);
        },
        function(sess, cb){
            console.log('trying to get timetable...');
            tryAgain(url, {tr: trCnt, result: 'OK', append: {sess: 1}}, cb);
        },
        function(res, cb){
            if (res.result == 'OK'){
                var ret = [];
                _.each(res.tp[0].list, function(train){
                    var t = {
                        number: train.number,
                        dtStart: moment(train.date0 + ' ' + train.time0, 'DD.MM.YYYY HH:mm').toDate(),
                        dtEnd: moment(train.date1 + ' ' + train.time1, 'DD.MM.YYYY HH:mm').toDate(),
                        seats: []
                    };
                    _.each(train.cars, function(el){
                        t.seats.push({type:el.type, free: el.freeSeats, price: el.tariff});
                    });
                    ret.push(t);
                });
                cb(null, ret);
            }else{
                cb('error when request tt');
            }
        }
    ], cb);
}

var station;
var arr;
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
        async.parallel({
            from: function(cb){
                getStationId(cfg.train.from, cb)
            },
            to: function(cb){
                getStationId(cfg.train.to, cb)
            }}, cb);
    },
    function(st, cb){
        station = st;
        var dtObj = {};
        var sDate = new moment();
        var qEnd = new moment(sDate).add(45, 'days').startOf('day');
        while (sDate.format('YYYY-MM-DD') != qEnd.format('YYYY-MM-DD')){
            var fDate = sDate.format('YYYY-MM-DD');
            if (!dtObj[fDate]){
                dtObj[fDate] = {
                    date: new moment(sDate).startOf('day').toDate()
                };
            }
            sDate.add(1, 'days');
        }
        arr = _.toArray(dtObj);
        var z = arr.length;
        console.time('total');
        db.collection("trains", safe.sure(cb, function (trainsCol) {
            async.eachLimit(arr, 1, function(el, cb){
                console.log('--- getting data for '+moment(el.date).format('YYYY-MM-DD')+' ---');
                console.time('done');
                var tt = [];
                async.waterfall([
                    function(cb){
                        console.log('getting direct');
                        getTrainTimetable(station.from, station.to, el.date, cb);
                    },
                    function(_tt, cb){
                        tt.push({dir:{from: station.from,
                            to: station.to}, timetable: _tt});
                        setTimeout(cb, 3000);
                    },
                    function(cb){
                        console.log('getting reverse');
                        getTrainTimetable(station.to, station.from, el.date, cb);
                    }
                ], function(err, _tt){
                    if (!err){
                        console.log('success!');
                        tt.push({dir:{from: station.to,
                            to: station.from}, timetable: _tt});
                        el.trains = tt;

                    }else{
                        console.log('can not get this day. '+ err);
                    }
                    z--;

                    if (el.trains){
                        console.log('inserting into the mongo');
                        el.insertTime = new Date();
                        trainsCol.insert(el, safe.sure(cb, function(){
                            console.timeEnd('done');
                            setTimeout(cb, 4000);
                        }));
                    } else {
                        console.timeEnd('done');
                        setTimeout(cb, 4000);
                    }
                    console.log(parseInt((1-(z / 45))*100).toString()+'% complete. Sleeping..');
                });
            }, cb);
        }));
    }
], function(err){
    if (err) console.log(err);
    process.exit();
});