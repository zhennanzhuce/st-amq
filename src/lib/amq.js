/*!
 * speedt-amq
 * Copyright(c) 2017 speedt <13837186852@qq.com>
 * MIT Licensed
 */
'use strict';

const path   = require('path');
const cwd    = process.cwd();
const conf   = require(path.join(cwd, 'settings'));

const Stomp  = require('stompjs');

const activemq = conf.activemq;

exports.send = function(dest, params, data, cb){
  this.getClient((err, client) => {
    if(err) return cb(err);
    try{
      client.send(dest, params || {}, JSON.stringify(data));
      cb();
    }catch(ex){ cb(ex); }
  });
};

(() => {
  var client = null;

  function unsubscribe(){
    if(!client) return;
    client.disconnect(() => {});
  }

  process.on('SIGTERM', unsubscribe);
  process.on('exit',    unsubscribe);

  exports.getClient = function(cb){
    if(client) return cb(null, client);
    cb(new Error('wait reconnect'));
  };

  exports.start = function(params, cb){
    var self = this;

    (function schedule(second){
      second = 1000 * (second || 3);

      var timeout = setTimeout(() => {
        clearTimeout(timeout);

        if(client && client.connected) return schedule(1);

        client = Stomp.overTCP(activemq.host, activemq.port);
        client.heartbeat.outgoing = 20000;
        client.heartbeat.incoming = 20000;

        client.connect({
          login:    activemq.user,
          passcode: activemq.password,
        }, () => {

          for(let i in params){
            let fn = params[i];
            if(client) client.subscribe(i, fn.bind(null, self.send.bind(self)))
          }

        }, err => {
          client = null;
          cb(err);
        });

        schedule(1);
      }, second);
    })();
  };
})();
