"use strict";
var fs = require("fs");
var _ = require("lodash");
var cfg;
var Config = function() {
    if (! cfg) {
        var defConfig;
        var localConfig;
        if (fs.existsSync(__dirname + "/config/config.json")) {
            defConfig = fs.readFileSync(__dirname + "/config/config.json");
            defConfig = JSON.parse(defConfig);
        } else {
            return null;
        }
        if (fs.existsSync(__dirname + "/config/local-config.json")) {
            localConfig = fs.readFileSync(__dirname + "/config/local-config.json");
            localConfig = JSON.parse(localConfig);
        }
        cfg = _.merge(defConfig, localConfig);
    }
    return cfg;
};
module.exports = Config;