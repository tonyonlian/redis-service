const Redis = require('ioredis');
const GenericPool = require('generic-pool');



function RedisInit(option){
  //处理配置信息
  let _hosts = [];
  option.sentinels.forEach(function(host_port) {
       let _tempArr =  host_port.split(':');
       let _tempobj = {host:_tempArr[0],port:_tempArr[1]};
       _hosts.push(_tempobj);
  }); 

  /**
   *  opts ： {"sentinels":[{ "host": "l4.qinglight.com","port": 8080 }],"name": "mymaster"}
   */

  let opts = {sentinels:_hosts,name:option.name};
  if(option.password){
         opts.password = option.password;
  }

    this.factory={
            create:function(){
                    return new Promise(function(resolve,reject){
                            var client = new Redis(opts); //配置获取信息
                            //console.log("start cononect redis server...");
                            client.on('connect',function(){
                                //console.log("successfully cononect redis server");
                                resolve(client); 
                            });
                            client.on('error',function(){
                                    reject();
                            });

                    });
            },
            destroy:function(client){
                    return new Promise(function(resolve,reject){
                            client.on('end',function(){
                                    resolve();
                            });
                            client.on('error',function(){
                                    reject();
                            });
                            client.disconnect();
                    });
            },
            validate:function(client){
                    console.log("validate");
                    return new Promise(function(resolve){

                            client.ping().then(function(result){
                                if(result == ping ){
                                    resolve(true);
                                }else{
                                    resolve(false);   
                                }
                            });
                            

                    });
            }

    };
    this.opt ={  
            max:10,
            maxWaitingClients:1000,
            min:2
    }

    this.redisPool = GenericPool.createPool(this.factory, this.opt);
   
}


RedisInit.prototype.get_redis_conn = function(){
        _this = this;
        return _this.redisPool.acquire();
}


RedisInit.prototype.close_redis_conn = function(client){
        _this = this;
        if(client){   
            _this.redisPool.release(client).catch(function(err){
                    console.log(err);
            });
        }
}

module.exports = RedisInit;