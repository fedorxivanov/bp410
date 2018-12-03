const sqlConnectionPool = require('tedious-connection-pool')
const sqlRequest = require('tedious').Request
const events = require('events')
class emitter extends events {}
const eventHandler = new emitter()

const sqlPoolConfig = {
    min: 2,
    log: false
}

const sqlConnectionConfig = {
    userName: 'fivanov_bpcli410',
    password: '',
    server: '10.192.1.68',
    port: 1433,
    options: {
        requestTimeout: 0
    }
}

const sqlPool = new sqlConnectionPool(sqlPoolConfig, sqlConnectionConfig)


sqlPool.on('error', err => {

    throw err
})

runTransaction('tran001',
"delete from bpcli410 where varcharCol01 like '%1%'",
"insert into bpcli410 select * from bpcli410 where intCol like '%3%'")

eventHandler.on('tran001::deleteCompleted', () => {

    runTransaction('tran002',
    "delete from bpcli410 where varcharCol01 like '%2%'",
    "insert into bpcli410 select * from bpcli410 where intCol like '%4%'",
    //exitNode
    )
})


function runTransaction (transactionName, deleteSqlStatement, insertSqlStatement, callback) {

    sqlPool.acquire((err, acquiredConnection) => {

        if (err) {
    
            logError(err)
        }
        console.info(`${new Date() .toISOString()} :: ${transactionName} :: connection acquired`)
        let deleteRequest = new sqlRequest(deleteSqlStatement, (err, rowCount) => {
    
            if (err) {
    
                logError(err)
            }
            eventHandler.emit(`${transactionName}::deleteCompleted`)
            console.info(`${new Date() .toISOString()} :: ${transactionName} :: "${arguments[1]}" completed. Rows affected: ` + rowCount)
            acquiredConnection.execSqlBatch(insertRequest)
            console.info(`${new Date() .toISOString()} :: ${transactionName} :: "${arguments[2]}" started`)
        })
        let insertRequest = new sqlRequest(insertSqlStatement, (err, rowCount) => {
    
            if (err) {
    
                logError(err)
            }
            console.info(`${new Date() .toISOString()} :: ${transactionName} :: "${arguments[2]}" completed. Rows affected: ` + rowCount)
            acquiredConnection.rollbackTransaction(err => {
    
                if (err) {
    
                    logError(err)
                }
                console.info(`${new Date() .toISOString()} :: ${transactionName} :: rollback`)
                if (callback) {

                    callback()
                }
            }, transactionName)
        })
    
        acquiredConnection.beginTransaction(err => {
    
            if (err) {
    
                logError(err)
            }
            console.info(`${new Date() .toISOString()} :: ${transactionName} :: begin`)
            acquiredConnection.execSqlBatch(deleteRequest)
            eventHandler.emit(`${transactionName}::deleteStarted`)
            console.info(`${new Date() .toISOString()} :: ${transactionName} :: "${arguments[1]}" started`)
        }, transactionName)
    })
}

function exitNode() {

    console.info(`${new Date() .toISOString()} :: terminating NodeJS process`)
    setImmediate(process.exit())
}

function logError(error) {

    console.info(`${new Date() .toISOString()} :: ERROR :: ${error}`)
}