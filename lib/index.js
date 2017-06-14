const RedisInit = require('../lib/db.js');
const Hoek = require('hoek');
const Joi = require('joi');
var rediser = '';

var cacheService = {};

cacheService.set = function(key,data){

    let dataStr = typeof data === 'string'? data:JSON.stringify(data);
    return _execRedisFunc('set',key,dataStr);
}

cacheService.setTtl = function(key,data,expire){
    
     let  dataStr = typeof data === 'string'? data:JSON.stringify(data);
     return _execRedisFunc('setex',key,expire,dataStr);
    
}

cacheService.get = function(key){
     return _execRedisFunc('get',key); 
}



cacheService.remove = function(key){
    return _execRedisFunc('del',key);
   
    }

   cacheService.getExpireTime = function(key){
         return _execRedisFunc('ttl',key);
   }

   function  _execRedisFunc(type,...obj){
        let redisConn = rediser.get_redis_conn();
            return new Promise(function(resolve,reject){
                redisConn.then(function(client){
                    client[type](obj).then(function(result){
                        var ret = result;
                        try{
                          ret = typeof result === 'string'?JSON.parse(result):result;
                        }catch(err){
                          ret = result;
                        }
                        resolve(ret);
                    });
                rediser.close_redis_conn(client);
                });
            
            });
      
   }

/**
 * 封装消息队列
 */
 

var events = require('events');
var  util = require('util');

function MessageQue(){
    //this.channel = null;
} 

util.inherits(MessageQue,events.EventEmitter);
MessageQue.prototype.reiceiver = function(message){
    var _this = this;
        //setTimeout(function(){
        _this.emit('message',message);
    // },2000);    
}
    MessageQue.prototype.sender = function(channel,data){
    var _this = this;
    let  dataStr = typeof data === 'string'? data:JSON.stringify(data);
    return _execRedisFunc('publish',channel,dataStr); 
}

MessageQue.prototype.onMessage = function(channel,callback){
    var _this = this;
    var _errMessage = {error:'onMessage error'};
    _this.on('message',function(message){
            //_this.channel = channel;
            if( channel && util.isArray(channel)){
            if(channel.length == 0){
                    callback(null,message);
            }else{
                if(channel.indexOf(message.channel)>-1){
                    callback(null,message);
                }
            }
            }else{
            _errMessage.error = ' param ' + channel + ' on array'; 
            return callback(_errMessage);
            }
        
    });
    
}



var messageQue = new MessageQue();

//为 'error' 事件注册监听器 防止发生错误nodejs进程崩溃
messageQue.on('error', function(err)  {
   console.error('MessageQue on error:'+ err);
});

function _messageQueInit(rediser,server,options){ 
    if(options.channels && util.isArray(options.channels) && options.channels.length>0 ){
        let redisConn = rediser.get_redis_conn();
        redisConn.then(function(client){
            //订阅配置的channel
            client.subscribe(options.channels,function (err, count) {
                if(err){
                    server.log('error','subscribe channel error' + JSON.stringify(err));
                    return;
                }
                server.log('info','subscribe channel:' + options.channels +' channel Num: '+count );
                
            });
            
            //监听获取消息
            client.on('message', function (channel, message) {
                var _obj = {channel:channel,message:message};
                server.log('info','Receive message '+message+' from channel ' + channel );
                server.log('info','inject message into MessageQue' );
                messageQue.reiceiver(_obj);
                
            });
        });
    }else{
        server.log('info','subscribe channel: []' ); 
    }
}


var serivce = {}

/**
 * 导出插件
 */

let default_options = {
    sentinels:["l4.qinglight.com:8080"],
    name: "mymaster",
    cache:true,
    mq:false,
    channels:["channel1"],
}

const schema = Joi.object().keys({
     sentinels: Joi.array().items(Joi.string()).single().required(),
     name:Joi.string(),
     password:Joi.string(),
     cache:Joi.boolean(),
     mq:Joi.boolean(),
     channels:Joi.array().items(Joi.string()).single()
 })

exports.register = function (server, opts, next) {
    
    const result = Joi.validate(opts, schema);
    if(result.error){
        console.error(result.error);
        return;
    }
    const options = Hoek.applyToDefaults(default_options, opts);

    rediser = new RedisInit(options);
    if(options.mq){
       _messageQueInit(rediser,server,options);
       serivce.mq = messageQue;
    }

    if(options.cache){
        serivce.cache = cacheService;
    }

    server.expose('serivce', serivce);
    server.log('info','completed to mysql-service plugs init');
    return  next();
};

exports.register.attributes = {
    pkg: require('../package.json')
};
