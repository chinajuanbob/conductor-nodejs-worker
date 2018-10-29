const ConductorWorker = require('./index.js')

const worker = new ConductorWorker({
    url: 'http://localhost:8080', // host
    apiPath: '/api', // base path
    workerid: 'node-worker',
})

const taskType = 'test'

const fn = (input) => {
    return new Promise((resolve, reject) => {
      const handler = setTimeout(()=>{
        clearTimeout(handler)
        resolve({
            result: false,
        })
      }, 3000)
    })
  }

worker.Start(taskType, fn)

setTimeout(()=>{
  worker.Stop()
}, 10000)