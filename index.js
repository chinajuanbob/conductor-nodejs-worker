const util = require('util')
const EventEmitter = require('events').EventEmitter
const clients = require('restify-clients')
const pForever = require('p-forever')
const sleep = require('sleep-promise')
const debug = require('debug')('conductor-nodejs-worker:debug')
const info = require('debug')('conductor-nodejs-worker:info')
const trace = require('debug')('conductor-nodejs-worker:trace')

function ConductorWorker(options) {
    EventEmitter.call(this)
    this.url = options.url
    this.apiPath = options.apiPath
    this.workerid = options.workerid
    this.client = clients.createJsonClient({
        url: this.url,
    })
}
util.inherits(ConductorWorker, EventEmitter)

module.exports = ConductorWorker

ConductorWorker.prototype.pollAndWork = function (taskType, fn) { // keep 'function'
  trace('ConductorWorker.pollAndWork called')
  const that = this
  return new Promise((resolve, reject) => {
    debug('Polling task to work from %o', `${that.apiPath}/tasks/poll/${taskType}?workerid=${that.workerid}`)    
    that.client.get(`${that.apiPath}/tasks/poll/${taskType}?workerid=${that.workerid}`, (err, req, res, obj) => {
      if (err) {
        reject(err)
        return
      }

      info('Pooling task %o', taskType)
      if (res.statusCode === 204) {
        resolve(null)
        return
      }
      
      debug('Task object retrieved %o', obj)
      const input = obj.inputData || {}
      const { workflowInstanceId, taskId } = obj
      that.client.post(`${that.apiPath}/tasks/${taskId}/ack?workerid=${that.workerid}`, {}, (err, req, res, obj) => {
        if (err){
          debug('HTTP ERROR ON ACK TASK REQUEST')
          reject(err)
          return
        }
        // console.log('ack?: %j', obj)
        if (obj !== true) {
          resolve({
            reason: 'FAILED_ACK',
            workflowInstanceId,
            taskId
          })
          return
        }
        const t1 = Date.now()
        const result = {
          workflowInstanceId,
          taskId,
        }
        fn(input).then(output => {
          result.callbackAfterSeconds = (Date.now() - t1)/1000
          result.outputData = output
          result.status ='COMPLETED'
          that.client.post(`${that.apiPath}/tasks/`, result, (err, req, res) => {
            if(res.statusCode >= 300){
              debug('HTTP ERROR ON COMPLETE TASK REQUEST')
              reject(err)
              return
            }
            
            resolve({
              reason: 'COMPLETED',
              workflowInstanceId,
              taskId
            })

          })
        }, (err) => {
          result.callbackAfterSeconds = (Date.now() - t1)/1000
          result.reasonForIncompletion = err // If failed, reason for failure
          result.status ='FAILED'
          that.client.post(`${that.apiPath}/tasks/`, result, (err, req, res, obj) => {
            if(res.statusCode >= 300){
              debug('HTTP ERROR ON POST FAILED TASK REQUEST')
              reject(err)
              return
            }
            resolve({
              reason: 'FAILED',
              workflowInstanceId,
              taskId
            })
          })
        })
      })
    })
  })
}

ConductorWorker.prototype.Start = function (taskType, fn, interval) {
  trace('ConductorWorker.Start called')
  const that = this
  this.working = true
  pForever(async () => {
    if (that.working) {
      debug('Sleeping for %o ms', interval || 1000)
      await sleep(interval || 1000)
      return that.pollAndWork(taskType, fn).then(data => {
        if(data!=null) {
          info('%o workflowInstanceId=%o; taskId=%o ', data.reason, data.workflowInstanceId, data.taskId)
        } else {
          info('The task pool is empty! Retrying soon...')
        }
      }, (err) => {
        info('Communication Failure: %s', err)
      })
    } else {
      return pForever.end
    }
  })
}

ConductorWorker.prototype.Stop = function (taskType, fn) {
  trace('ConductorWorker.Stop called')
  this.working = false
}
