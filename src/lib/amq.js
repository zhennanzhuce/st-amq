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

(() => {
  var client = null;

  function unsubscribe(){
    if(!client) return;

    client.disconnect(() => {
      for(let i of fns){ i.unsubscribe(); }
    });
  }

  process.on('SIGTERM', unsubscribe);
  process.on('exit',    unsubscribe);

  exports.getClient = function(cb){
    if(client) return cb(null, client);

    client = Stomp.overTCP(activemq.host, activemq.port);
    client.heartbeat.outgoing = 20000;
    client.heartbeat.incoming = 10000;

    client.connect({
      login:    activemq.user,
      passcode: activemq.password,
    }, () => {
      cb(null, client);
    }, err => {
      if(!client) return cb(err);
      client.disconnect(cb.bind(null, err));
    });
  };

  var fns = [];

  /**
   * 注入监听队列
   *
   * @return
   */
  exports.injection = function(name, fn, cb){
    var self = this;

    self.getClient((err, client) => {
      if(err) return cb(err);
      if(!client) return cb(new Error('no client'));
      fns.push(client.subscribe(name, fn.bind(null, self.send.bind(self))));
      cb();
    });
  };
})();

exports.send = function(dest, params, data, cb){
  this.getClient((err, client) => {
    if(err) return cb(err);
    try{
      client.send(dest, params || {}, JSON.stringify(data));
      cb(null, 'OK');
    }catch(ex){ cb(ex); }
  });
};
