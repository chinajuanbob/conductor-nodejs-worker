const util = require('util')
const EventEmitter = require('events').EventEmitter
const clients = require('restify-clients')
const pForever = require('p-forever')
const sleep = require('sleep-promise')
 
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
  const that = this
  return new Promise((resolve, reject) => {
    that.client.get(`${that.apiPath}/tasks/poll/${taskType}?workerid=${that.workerid}`, (err, req, res, obj) => {
      if (err){
        reject(err)
        return
      }
      if (!obj || !obj.inputData) {
        resolve(null)
        return
      }
      const input = obj.inputData
      const { workflowInstanceId, taskId } = obj
      that.client.post(`${that.apiPath}/tasks/${taskId}/ack?workerid=${that.workerid}`, (err, req, res, obj) => {
        if (err){
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
          that.client.post(`${that.apiPath}/tasks/`, result, (err, req, res, obj) => {
            // err is RestError: Invalid JSON in response, ignore it
            // console.log(obj)
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
            // err is RestError: Invalid JSON in response, ignore it
            // console.log(obj)
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
  const that = this
  this.working = true
  pForever(async () => {
    if (that.working) {
      await sleep(interval || 1000)
      return that.pollAndWork(taskType, fn).then(data => {
        if(data!=null) {
          console.log(data.reason + ' workflowInstanceId=' + data.workflowInstanceId + '; taskId=' + data.taskId)
        }
      }, (err) => {
        console.log(err)
      })
    } else {
      return pForever.end
    }
  })
}

ConductorWorker.prototype.Stop = function (taskType, fn) {
  this.working = false
}
